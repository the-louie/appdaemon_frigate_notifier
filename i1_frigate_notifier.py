"""
Frigate Notification App for AppDaemon

Copyright (c) 2025 the_louie
All rights reserved.

This app listens to Frigate MQTT events and sends notifications to configured users
when motion is detected. It supports zone filtering, cooldown periods, and snapshot
image downloads. Notifications include image attachments and action links to view
the corresponding video clips.

Configuration:
  frigate_notify:
    module: i1_frigate_notifier
    class: FrigateNotification
    mqtt_topic: "frigate/events"
    frigate_url: "https://frigate.example.com/api/events"
    ext_domain: "https://your-domain.com"
    snapshot_dir: "/path/to/snapshots"
    only_zones: true
    persons:
      - name: user1
        notify: mobile_app_device
        labels: ["person", "car"]
        cooldown: 120
    cam_icons:
      camera1: "mdi:doorbell-video"
      camera2: "mdi:car-estate"
"""

import hashlib
import json
import random
import sys
import threading
import time
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import appdaemon.plugins.hass.hassapi as hass


class FrigateNotification(hass.Hass):
    """AppDaemon app for sending Frigate motion notifications."""

    def _calculate_stats(self, times: List[float]) -> Dict[str, float]:
        """Calculate statistical metrics for a list of times."""
        if not times:
            return {}

        mean = sum(times) / len(times)
        std_dev = (sum((x - mean) ** 2 for x in times) / len(times)) ** 0.5 if len(times) >= 2 else 0.0

        return {
            "min": round(min(times), 3),
            "avg": round(mean, 3),
            "max": round(max(times), 3),
            "std": round(std_dev, 3)
        }

    def _create_metrics_dict(self, times: List[float], metric_type: str, include_raw: bool = False) -> Dict[str, Any]:
        """Create metrics dictionary with optional raw data."""
        if not times:
            return {}

        stats = self._calculate_stats(times)
        result = {
            f"total_{metric_type}": len(times),
            f"min_{metric_type}_seconds": stats["min"],
            f"avg_{metric_type}_seconds": stats["avg"],
            f"max_{metric_type}_seconds": stats["max"],
            f"std_{metric_type}_seconds": stats["std"]
        }

        if include_raw:
            result["raw"] = times

        return result

    def _extract_metrics_from_entry(self, entry: Dict[str, Any], metric_type: str) -> Dict[str, Any]:
        """Extract metrics from entry in either old or new format."""
        key = f"{metric_type}_times"
        if key not in entry:
            return {}

        data = entry[key]
        if isinstance(data, dict):
            # New format with calculated stats
            return {k: v for k, v in data.items() if not k == "raw"}

        # Old format - calculate stats from raw times
        return self._create_metrics_dict(data, metric_type)

    def initialize(self) -> None:
        """Initialize the Frigate notification app.

        Sets up MQTT connection, loads configuration, initializes metrics tracking,
        and starts the event processing thread and scheduled cleanup tasks.
        """
        self.msg_cooldown = {}
        self.person_configs = []
        self.event_queue = deque(maxlen=1000)
        self.queue_lock = threading.Lock()
        self.processing_thread = None
        self.shutdown_event = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="FrigateNotifier")
        self.file_cache = {}
        self.cache_lock = threading.Lock()

        # Performance metrics
        self.notification_times = []
        self.delivery_times = []
        self.metrics_lock = threading.Lock()
        self.metrics_file = Path(__file__).parent / "notification_metrics.json"

        # Notification tracking to prevent duplicates
        self.notified_events = set()  # Track events that have already been notified
        self.notification_lock = threading.Lock()
        
        # Circuit breaker for download failures
        # Circuit breaker state: endpoint -> {"failures": count, "last_failure": timestamp, "state": "closed"|"open"|"half_open"}
        self.circuit_breaker_state = {}
        self.circuit_breaker_lock = threading.Lock()

        # Load today's metrics on startup
        self._load_todays_metrics()

        self._load_config()
        self._setup_mqtt()

        # Set up notification delivery tracking
        self.listen_event(self._handle_notification_received, "mobile_app_notification_received")

        # Start processing thread
        self.processing_thread = threading.Thread(
            target=self._event_processing_worker, name="EventProcessor", daemon=True
        )
        self.processing_thread.start()

        # Schedule periodic tasks
        self.run_every(self._cleanup_old_files, datetime.now(), 24 * 60 * 60)
        self.run_every(
            self._log_daily_metrics,
            datetime.now().replace(hour=23, minute=59, second=0, microsecond=0),
            24 * 60 * 60
        )
        self.run_every(self._cleanup_cache, datetime.now(), 6 * 60 * 60)
        self.run_every(self._cleanup_notified_events, datetime.now(), 60 * 60)  # Every hour
        self.run_every(self._cleanup_circuit_breakers, datetime.now(), 12 * 60 * 60)  # Every 12 hours

    def _load_config(self) -> None:
        """Load and validate configuration parameters.

        Validates required parameters (frigate_url, ext_domain), loads optional
        parameters with defaults, and sets up the snapshot directory if specified.
        """
        # Validate required parameters
        required_params = [("frigate_url", "frigate_url"), ("ext_domain", "ext_domain")]
        for param_name, attr_name in required_params:
            value = self.args.get(param_name)
            if not value:
                self.log(f"ERROR: {param_name} is required", level="ERROR")
                return
            setattr(self, attr_name, value)

        # Load optional parameters with defaults
        self.mqtt_topic = self.args.get("mqtt_topic", "frigate/events")
        self.only_zones = self.args.get("only_zones", False)
        self.cam_icons = self.args.get("cam_icons", {})
        self.max_file_age_days = self.args.get("max_file_age_days", 30)
        self.cache_ttl_hours = self.args.get("cache_ttl_hours", 24)
        self.connection_timeout = self.args.get("connection_timeout", 30)

        # Setup snapshot directory
        snapshot_dir = self.args.get("snapshot_dir")
        if snapshot_dir:
            self.snapshot_dir = Path(snapshot_dir)
            self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.snapshot_dir = None

        self._load_person_configs()

    def _load_person_configs(self) -> None:
        """Load and validate person notification configurations."""
        for person_data in self.args.get("persons", []):
            try:
                # Early validation - check required fields first
                name = person_data.get("name")
                notify = person_data.get("notify")
                labels = person_data.get("labels", [])

                if not name or not notify or not labels:
                    self.log(
                        f"ERROR: Missing required fields for person config: name={name}, notify={notify}, labels={labels}",
                        level="ERROR"
                    )
                    continue

                # Build config with validated data
                config = {
                    "name": name,
                    "notify": notify,
                    "labels": set(labels),
                    "cooldown": max(0, person_data.get("cooldown", 0)),  # Ensure non-negative
                    "enabled": person_data.get("enabled", True),
                    "zones": set(person_data.get("zones", [])) if person_data.get("zones") else None,
                    "cameras": set(person_data.get("cameras", [])) if person_data.get("cameras") else None
                }

                self.person_configs.append(config)

            except Exception as e:
                line_num = sys.exc_info()[2].tb_lineno
                self.log(f"ERROR: Failed to load person config: {e} (line {line_num})", level="ERROR")

    def _setup_mqtt(self) -> None:
        """Set up MQTT connection and subscribe to Frigate events.

        Establishes connection to MQTT broker, subscribes to the configured
        topic, and sets up the message event handler.
        """
        try:
            self.mqtt = self.get_plugin_api("MQTT")
            if self.mqtt.is_client_connected():
                self.mqtt.mqtt_subscribe(f"{self.mqtt_topic}/#")
                self.mqtt.listen_event(self._handle_mqtt_message, "MQTT_MESSAGE")
            else:
                self.log("ERROR: MQTT not connected", level="ERROR")
        except Exception as e:
            self.log(f"ERROR: Failed to set up MQTT: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _event_processing_worker(self) -> None:
        """Background worker thread for processing queued events.

        Continuously processes events from the queue until shutdown is signaled.
        Handles exceptions to prevent worker thread termination.
        """
        while not self.shutdown_event.is_set():
            try:
                with self.queue_lock:
                    if self.event_queue:
                        self._process_event(self.event_queue.popleft())
                    else:
                        time.sleep(0.1)
            except Exception as e:
                line_num = sys.exc_info()[2].tb_lineno
                self.log(f"ERROR: Event processing worker error: {e} (line {line_num})", level="ERROR")

    def _handle_mqtt_message(self, event_name: str, data: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        """Handle incoming MQTT messages from Frigate."""
        try:
            # Early validation - exit immediately if invalid
            if not data or 'topic' not in data or 'payload' not in data or not data['topic'].startswith(self.mqtt_topic):
                return

            # Parse JSON payload
            payload = data['payload']
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    self.log("ERROR: Invalid JSON payload", level="ERROR")
                    return

            # Extract event data and validate it's an end event
            event_data = self._extract_event_data(payload)
            if not event_data or event_data["event_type"] != "end":
                return

            event_id = event_data["event_id"]
            event_suffix = event_id.split('-')[-1] if '-' in event_id else event_id

            # Early exit if no potential recipients
            if not self._has_potential_recipients(event_data):
                return

            # Queue event for processing
            with self.queue_lock:
                if len(self.event_queue) < 1000:
                    self.event_queue.append(event_data)
                    self.log(f"DEBUG[{event_suffix}]: Event queued (size: {len(self.event_queue)})")
                else:
                    self.log(f"DEBUG[{event_suffix}]: Event queue full, dropping event")

        except Exception as e:
            line_num = sys.exc_info()[2].tb_lineno
            self.log(f"ERROR: Failed to process MQTT message: {e} (line {line_num})", level="ERROR")

    def _handle_notification_received(self, event_name: str, data: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        """Handle mobile app notification received events to track delivery times."""
        try:
            if not data or 'data' not in data:
                return

            notification_data = data['data']
            if not notification_data or 'timestamp' not in notification_data:
                return

            # Parse the original timestamp from the notification
            try:
                original_timestamp = datetime.strptime(notification_data['timestamp'], "%H:%M:%S")
                # Use today's date since we only have time
                today = datetime.now().date()
                original_datetime = datetime.combine(today, original_timestamp.time())

                # Calculate delivery time
                current_time = datetime.now()
                delivery_time = (current_time - original_datetime).total_seconds()

                # Handle cross-midnight scenario (if delivery time is negative, assume previous day)
                if delivery_time < 0:
                    original_datetime = datetime.combine(today - timedelta(days=1), original_timestamp.time())
                    delivery_time = (current_time - original_datetime).total_seconds()

                # Only record if delivery time is reasonable (positive and less than 1 hour)
                if 0 <= delivery_time <= 3600:
                    with self.metrics_lock:
                        self.delivery_times.append(delivery_time)
                        # Save metrics immediately after each delivery time
                        self._save_todays_metrics()

                    event_id = notification_data.get('event_id', 'unknown')
                    self.log(f"Notification delivered in {delivery_time:.3f}s - Event ID: {event_id}")

            except ValueError as e:
                line_num = sys.exc_info()[2].tb_lineno
                self.log(f"ERROR: Failed to parse notification timestamp: {e} (line {line_num})", level="ERROR")

        except Exception as e:
            line_num = sys.exc_info()[2].tb_lineno
            self.log(f"ERROR: Failed to process notification received event: {e} (line {line_num})", level="ERROR")

    def _extract_event_data(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract and validate event data from payload."""
        try:
            event_data = payload.get("after", {})
            event_id = event_data.get("id")
            if not event_id:
                return None

            camera = event_data.get("camera", "Unknown")
            label = event_data.get("label", "Unknown")
            entered_zones = event_data.get("entered_zones", [])

            return {
                "event_id": event_id,
                "camera": camera,
                "label": label,
                "entered_zones": entered_zones,
                "event_type": payload.get("type", ""),
                "timestamp": datetime.now()
            }

        except Exception as e:
            self.log(f"ERROR: Failed to extract event data: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")
            return None

    def _process_event(self, event_data: Dict[str, Any]) -> None:
        """Process a Frigate event."""
        event_id = event_data["event_id"]
        event_suffix = event_id.split('-')[-1] if '-' in event_id else event_id

        try:
            # Skip zone filtering check if only_zones is disabled or zones exist
            if self.only_zones and not event_data["entered_zones"]:
                self.log(f"DEBUG[{event_suffix}]: Skipping - only_zones enabled but no zones entered")
                return

            self.log(f"DEBUG[{event_suffix}]: Processing {event_data['camera']}/{event_data['label']} event")
            self.executor.submit(self._download_and_notify, event_data)

        except Exception as e:
            self.log(f"ERROR: Failed to process event: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _download_and_notify(self, event_data: Dict[str, Any]) -> None:
        """Download media and send notifications for a Frigate event.

        Args:
            event_data: Dictionary containing event information including
                       event_id, camera, label, and entered_zones.

        Attempts to download snapshot image with retry logic, then sends
        notifications to all eligible recipients with the media attached.
        """
        event_id = event_data["event_id"]
        event_suffix = event_id.split('-')[-1] if '-' in event_id else event_id

        try:
            # Try to download snapshot image with intelligent retry
            media_path = self._download_media_with_retry(
                event_data["event_id"], event_data["camera"], "snapshot.jpg", ".jpg", 15, 1, event_suffix
            )
            if media_path:
                self._send_notifications(event_data, media_path, "image", event_suffix)
                return

            # Send notification without media if snapshot download failed
            self._send_notifications(event_data, None, None, event_suffix)

        except Exception as e:
            line_num = sys.exc_info()[2].tb_lineno
            self.log(f"ERROR: Failed to download and notify for event {event_id}: {e} (line {line_num})", level="ERROR")

    def _has_potential_recipients(self, event_data: Dict[str, Any]) -> bool:
        """Check if any user would receive notifications for this event."""
        if not self.person_configs:
            return False

        camera = event_data["camera"]
        label = event_data["label"]
        entered_zones = event_data["entered_zones"]
        current_time = time.time()

        for config in self.person_configs:
            # Skip if user disabled, wrong label, camera, or zone
            if (not config["enabled"] or
                label not in config["labels"] or
                (config["cameras"] and camera not in config["cameras"]) or
                (config["zones"] and not any(zone in config["zones"] for zone in entered_zones))):
                continue

            # Check cooldown period
            cooldown_key = f"{config['notify']}/{camera}"
            if current_time - self.msg_cooldown.get(cooldown_key, 0) >= config["cooldown"]:
                return True

        return False

    def _should_notify_user(self, config: Dict[str, Any], event_data: Dict[str, Any], current_time: float) -> bool:
        """Check if a user should receive a notification for this event."""
        camera = event_data["camera"]
        label = event_data["label"]
        entered_zones = event_data["entered_zones"]

        # Check user enabled, label match, camera match, and zone match
        if (not config["enabled"] or
            label not in config["labels"] or
            (config["cameras"] and camera not in config["cameras"]) or
            (config["zones"] and not any(zone in config["zones"] for zone in entered_zones))):
            return False

        # Check cooldown period
        cooldown_key = f"{config['notify']}/{camera}"
        return current_time - self.msg_cooldown.get(cooldown_key, 0) >= config["cooldown"]

    def _is_circuit_closed(self, circuit_key: str) -> bool:
        """Check if circuit breaker is closed (allowing requests)."""
        with self.circuit_breaker_lock:
            if circuit_key not in self.circuit_breaker_state:
                return True
                
            state_info = self.circuit_breaker_state[circuit_key]
            current_time = time.time()
            
            # Circuit breaker states:
            # - closed: normal operation
            # - open: blocking requests due to failures
            # - half_open: allowing single test request
            
            if state_info["state"] == "closed":
                return True
            elif state_info["state"] == "open":
                # Check if enough time has passed to try half-open
                if current_time - state_info["last_failure"] > 60:  # 60 second timeout
                    state_info["state"] = "half_open"
                    return True
                return False
            elif state_info["state"] == "half_open":
                return True
                
        return True
    
    def _record_circuit_failure(self, circuit_key: str) -> None:
        """Record a failure for circuit breaker tracking."""
        with self.circuit_breaker_lock:
            if circuit_key not in self.circuit_breaker_state:
                self.circuit_breaker_state[circuit_key] = {
                    "failures": 0,
                    "last_failure": 0,
                    "state": "closed"
                }
                
            state_info = self.circuit_breaker_state[circuit_key]
            state_info["failures"] += 1
            state_info["last_failure"] = time.time()
            
            # Open circuit if too many failures (5 failures triggers open state)
            if state_info["failures"] >= 5:
                state_info["state"] = "open"
                self.log(f"Circuit breaker OPENED for {circuit_key} due to {state_info['failures']} failures")
    
    def _reset_circuit_breaker(self, circuit_key: str) -> None:
        """Reset circuit breaker after successful operation."""
        with self.circuit_breaker_lock:
            if circuit_key in self.circuit_breaker_state:
                self.circuit_breaker_state[circuit_key] = {
                    "failures": 0,
                    "last_failure": 0,
                    "state": "closed"
                }
    
    def _is_retryable_error(self, error: Exception) -> bool:
        """Determine if an error is retryable based on error type and message."""
        error_str = str(error).lower()
        error_type = type(error).__name__
        
        # Network-related errors that are typically retryable
        retryable_errors = {
            "TimeoutError", "ConnectionError", "URLError", "HTTPError"
        }
        
        # HTTP status codes that are retryable
        retryable_http_statuses = {500, 502, 503, 504, 429}  # Server errors and rate limiting
        
        # Check for specific retryable conditions
        if error_type in retryable_errors:
            return True
            
        # Check for HTTP status codes in error message
        for status in retryable_http_statuses:
            if f"http error {status}" in error_str:
                return True
                
        # Check for common retryable error messages
        retryable_messages = [
            "timeout", "connection", "network", "temporary", "server error",
            "service unavailable", "bad gateway", "gateway timeout"
        ]
        
        return any(msg in error_str for msg in retryable_messages)

    def _download_media_with_retry(
        self, event_id: str, camera: str, endpoint: str, extension: str,
        max_timeout: int, base_delay: int, event_suffix: str
    ) -> Optional[str]:
        """Download media with exponential backoff, jitter, and circuit breaker.
        
        Args:
            event_id: Frigate event ID
            camera: Camera name
            endpoint: API endpoint (e.g., 'snapshot.jpg')
            extension: File extension
            max_timeout: Maximum total time to spend on retries
            base_delay: Base delay for exponential backoff (ignored, kept for compatibility)
            event_suffix: Event suffix for logging
            
        Returns:
            Path to downloaded media file or None if failed
        """
        circuit_key = f"{camera}_{endpoint}"
        
        # Check circuit breaker state
        if not self._is_circuit_closed(circuit_key):
            self.log(f"Circuit breaker OPEN for {circuit_key}, skipping download")
            return None
            
        start_time = time.time()
        attempt = 0
        max_attempts = 5  # Maximum number of retry attempts

        while time.time() - start_time < max_timeout and attempt < max_attempts:
            attempt += 1
            
            try:
                media_path = self._download_media(event_id, camera, endpoint, extension, event_suffix)
                if media_path:
                    # Success - reset circuit breaker
                    self._reset_circuit_breaker(circuit_key)
                    return media_path
                    
            except Exception as e:
                line_num = sys.exc_info()[2].tb_lineno
                error_type = type(e).__name__
                
                # Determine if error is retryable
                is_retryable = self._is_retryable_error(e)
                
                self.log(
                    f"ERROR: Media download attempt {attempt} failed for {event_id} ({error_type}): "
                    f"{e} (line {line_num})",
                    level="ERROR"
                )
                
                # Record failure for circuit breaker
                self._record_circuit_failure(circuit_key)
                
                # If not retryable or max attempts reached, exit early
                if not is_retryable or attempt >= max_attempts:
                    break

            # Calculate exponential backoff with jitter if more attempts allowed
            if attempt < max_attempts and time.time() - start_time < max_timeout:
                # Exponential backoff: 1s, 2s, 4s, 8s
                base_delay_exp = min(2 ** (attempt - 1), 8)  # Cap at 8 seconds
                # Add jitter: ±25% of base delay
                jitter = random.uniform(-0.25, 0.25) * base_delay_exp
                delay = max(0.1, base_delay_exp + jitter)  # Minimum 0.1s delay
                
                self.log(f"Retrying {endpoint} download in {delay:.2f}s (attempt {attempt + 1})")
                time.sleep(delay)
                
        return None

    def _download_media(
        self, event_id: str, camera: str, endpoint: str, extension: str, event_suffix: str
    ) -> Optional[str]:
        """Download media file from Frigate."""
        if not self.snapshot_dir:
            self.log(f"ERROR[{event_suffix}]: No snapshot directory configured, cannot download {endpoint}")
            return None

        cache_key = f"{event_id}_{camera}_{endpoint}"

        # Check cache first
        with self.cache_lock:
            if cache_key in self.file_cache:
                cache_entry = self.file_cache[cache_key]
                cache_age = (datetime.now() - cache_entry["timestamp"]).total_seconds()
                if cache_age < self.cache_ttl_hours * 3600:
                    return cache_entry["file_path"]

        # Download new media
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H:%M:%S")
        date_dir = now.strftime("%Y-%m-%d")
        target_dir = self.snapshot_dir / camera / date_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{timestamp}--{event_id}{extension}"
        target_path = target_dir / filename

        if target_path.exists():
            # Add existing file to cache
            with self.cache_lock:
                file_size = target_path.stat().st_size
                self.file_cache[cache_key] = {
                    "file_path": str(target_path),
                    "timestamp": datetime.now(),
                    "size": file_size,
                    "checksum": hashlib.md5(f"{cache_key}_{file_size}".encode()).hexdigest()
                }
            return f"{camera}/{date_dir}/{filename}"

        media_url = f"{self.frigate_url}/{event_id}/{endpoint}"
        req = urllib.request.Request(media_url)
        req.add_header('User-Agent', 'FrigateNotifier/1.0')

        try:
            with urllib.request.urlopen(req, timeout=self.connection_timeout) as response:
                if response.status >= 400:
                    raise Exception(f"HTTP Error {response.status}")

                content = response.read()
                file_size = len(content)

                # Validate file size
                if file_size == 0:
                    raise ValueError("File is empty: 0 bytes")
                if file_size < 50000:  # Minimum 50KB for images
                    raise ValueError(f"File too small: {file_size} bytes")

                # Write file to disk
                with open(target_path, 'wb') as f:
                    f.write(content)

            # Cache the downloaded file
            with self.cache_lock:
                self.file_cache[cache_key] = {
                    "file_path": str(target_path),
                    "timestamp": datetime.now(),
                    "size": file_size,
                    "checksum": hashlib.md5(f"{cache_key}_{file_size}".encode()).hexdigest()
                }
            return f"{camera}/{date_dir}/{filename}"

        except Exception:
            # Clean up partial file on error
            if target_path.exists():
                target_path.unlink()
            raise

    def _send_notifications(
        self, event_data: Dict[str, Any], media_path: Optional[str],
        media_type: Optional[str], event_suffix: str
    ) -> None:
        """Send notifications to all eligible recipients.

        Args:
            event_data: Event information including ID, camera, label, zones
            media_path: Path to downloaded media file (if available)
            media_type: Type of media ('image' or None)
            event_suffix: Short event ID for logging

        Filters recipients based on their configuration (labels, zones, cameras,
        cooldown periods) and sends Rich notifications with action buttons.
        """
        if not self.person_configs:
            self.log(f"DEBUG[{event_suffix}]: No person configs found, skipping notifications")
            return

        event_id = event_data["event_id"]

        # Check if this event has already been notified to prevent duplicates
        with self.notification_lock:
            if event_id in self.notified_events:
                self.log(f"DEBUG[{event_suffix}]: Event already notified, skipping duplicate notification")
                return
            self.notified_events.add(event_id)

        notification_start = time.time()
        timestamp = event_data["timestamp"].strftime("%H:%M:%S")
        camera = event_data["camera"]
        label = event_data["label"]
        entered_zones = event_data["entered_zones"]
        zone_str = ", ".join(entered_zones) if entered_zones else "No zones"

        # Build notification data once
        camera_uri = f"homeassistant://navigate/dashboard-kameror/{camera}"
        video_uri = f"{self.ext_domain}/api/frigate/frigate/notifications/{event_id}/clip.mp4"
        channel = f"frigate-{camera}"

        notification_data = {
            "actions": [
                {"action": "URI", "title": "Open Camera", "uri": camera_uri},
                {"action": "URI", "title": "Video", "uri": video_uri}
            ],
            "channel": channel,
            "importance": "high",
            "visibility": "public",
            "priority": "high",
            "ttl": 0,
            "event_id": event_id,
            "timestamp": timestamp,
            "notification_icon": self.cam_icons.get(camera, "mdi:cctv"),
            "confirmation": True
        }

        # Add media to notification (image only now)
        # Add image to notification if available
        if media_path and self.ext_domain and media_type == "image":
            media_url = f"{self.ext_domain}/local/frigate/{media_path.lstrip('/')}"
            notification_data["image"] = media_url

        # Send notifications to eligible users
        notifications_sent = 0
        current_time = time.time()
        title = f"{label} @ {camera}"
        message = f"{timestamp} - {zone_str} (ID: {event_id})"
        media_info = f" - {media_type}: {media_path}" if media_path else " - no media"

        for config in self.person_configs:
            # Skip if user doesn't match criteria or is in cooldown
            if not self._should_notify_user(config, event_data, current_time):
                continue

            # Send notification and update cooldown
            self.call_service(f"notify/{config['notify']}", title=title, message=message, data=notification_data)
            cooldown_key = f"{config['notify']}/{camera}"
            self.msg_cooldown[cooldown_key] = current_time
            notifications_sent += 1

            self.log(f"Notification sent to {config['name']} - {title} - Event ID: {event_id}{media_info}")

        # Record metrics and log completion
        notification_time = time.time() - notification_start
        with self.metrics_lock:
            self.notification_times.append(notification_time)
            self._save_todays_metrics()

        if notifications_sent > 0:
            self.log(f"Sent {notifications_sent} notifications for {event_id} in {notification_time:.3f}s")

    def _log_daily_metrics(self, **kwargs) -> None:
        """Log daily notification performance metrics and enrich previous day's data."""
        try:
            # First, enrich yesterday's data with calculated statistics
            self._enrich_yesterdays_metrics()

            with self.metrics_lock:
                if not self.notification_times and not self.delivery_times:
                    self.log("METRICS: No notifications sent today")
                    return

                # Calculate statistics using consolidated method
                stats = {}
                if self.notification_times:
                    stats.update(self._create_metrics_dict(self.notification_times, "notification"))
                    # Add backward compatibility keys
                    stats["total_notifications"] = stats.get("total_notification", 0)
                    stats["min_seconds"] = stats.get("min_notification_seconds", 0)
                    stats["avg_seconds"] = stats.get("avg_notification_seconds", 0)
                    stats["max_seconds"] = stats.get("max_notification_seconds", 0)

                if self.delivery_times:
                    delivery_stats = self._create_metrics_dict(self.delivery_times, "delivery")
                    stats.update({
                        "total_deliveries": delivery_stats.get("total_delivery", 0),
                        "min_delivery_seconds": delivery_stats.get("min_delivery_seconds", 0),
                        "avg_delivery_seconds": delivery_stats.get("avg_delivery_seconds", 0),
                        "max_delivery_seconds": delivery_stats.get("max_delivery_seconds", 0)
                    })

                # Add comparison if yesterday's data exists
                yesterday_metrics = self._load_yesterday_metrics()
                comparison = {}
                if yesterday_metrics:
                    if "min_seconds" in stats and "min_seconds" in yesterday_metrics:
                        comparison.update({
                            "min_diff": round(stats["min_seconds"] - yesterday_metrics["min_seconds"], 3),
                            "avg_diff": round(stats["avg_seconds"] - yesterday_metrics["avg_seconds"], 3),
                            "max_diff": round(stats["max_seconds"] - yesterday_metrics["max_seconds"], 3)
                        })

                    if "min_delivery_seconds" in stats and "min_delivery_seconds" in yesterday_metrics:
                        comparison.update({
                            "min_delivery_diff": round(
                                stats["min_delivery_seconds"] - yesterday_metrics["min_delivery_seconds"], 3
                            ),
                            "avg_delivery_diff": round(
                                stats["avg_delivery_seconds"] - yesterday_metrics["avg_delivery_seconds"], 3
                            ),
                            "max_delivery_diff": round(
                                stats["max_delivery_seconds"] - yesterday_metrics["max_delivery_seconds"], 3
                            )
                        })

                # Log metrics (data is already saved continuously)

                # Build efficient log message
                log_components = ["METRICS:"]

                if "total_notifications" in stats:
                    log_components.append(
                        f"Notifications={stats['total_notifications']}, Min={stats['min_seconds']}s, "
                        f"Avg={stats['avg_seconds']}s, Max={stats['max_seconds']}s"
                    )

                if "total_deliveries" in stats:
                    log_components.append(
                        f"Deliveries={stats['total_deliveries']}, Min={stats['min_delivery_seconds']}s, "
                        f"Avg={stats['avg_delivery_seconds']}s, Max={stats['max_delivery_seconds']}s"
                    )

                # Add comparison data if available
                if comparison:
                    comp_data = []
                    if "min_diff" in comparison:
                        comp_data.append(
                            f"Min diff: {comparison['min_diff']:+0.3f}s, Avg diff: {comparison['avg_diff']:+0.3f}s, "
                            f"Max diff: {comparison['max_diff']:+0.3f}s"
                        )
                    if "min_delivery_diff" in comparison:
                        comp_data.append(
                            f"Delivery diff: Min {comparison['min_delivery_diff']:+0.3f}s, "
                            f"Avg {comparison['avg_delivery_diff']:+0.3f}s, Max {comparison['max_delivery_diff']:+0.3f}s"
                        )
                    if comp_data:
                        log_components.extend(comp_data)

                self.log(" | ".join(log_components))

                # Clear today's data for next day and save empty state
                self.notification_times.clear()
                self.delivery_times.clear()
                self._save_todays_metrics()

        except Exception as e:
            self.log(f"ERROR: Failed to log daily metrics: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _load_todays_metrics(self) -> None:
        """Load today's metrics from file on startup."""
        try:
            if not self.metrics_file.exists():
                self.log("DEBUG: No metrics file found, starting fresh")
                return

            with open(self.metrics_file, 'r') as f:
                data = json.load(f)

            today = datetime.now().strftime("%Y-%m-%d")
            for entry in data.get("daily_metrics", []):
                if entry.get("date") == today:
                    # Load today's data back into memory
                    if "notification_times" in entry:
                        self.notification_times = entry["notification_times"]
                    if "delivery_times" in entry:
                        self.delivery_times = entry["delivery_times"]

                    notif_count, delivery_count = len(self.notification_times), len(self.delivery_times)
                    self.log(
                        f"DEBUG: Loaded {notif_count} notification times and {delivery_count} delivery times "
                        f"from today's metrics"
                    )
                    return

            self.log("DEBUG: No today's metrics found in file, starting fresh")

        except Exception as e:
            self.log(f"ERROR: Failed to load today's metrics: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _save_todays_metrics(self) -> None:
        """Save today's current metrics to file."""
        try:
            today = datetime.now().strftime("%Y-%m-%d")

            # Load existing data
            if self.metrics_file.exists():
                with open(self.metrics_file, 'r') as f:
                    data = json.load(f)
            else:
                data = {"daily_metrics": []}

            # Find today's entry or create new one
            today_entry = None
            for entry in data["daily_metrics"]:
                if entry.get("date") == today:
                    today_entry = entry
                    break

            if not today_entry:
                today_entry = {"date": today}
                data["daily_metrics"].append(today_entry)

            # Update today's entry with current data
            today_entry["notification_times"] = self.notification_times.copy()
            today_entry["delivery_times"] = self.delivery_times.copy()

            # Keep only last 30 days
            if len(data["daily_metrics"]) > 30:
                data["daily_metrics"] = data["daily_metrics"][-30:]

            # Save to file
            with open(self.metrics_file, 'w') as f:
                json.dump(data, f, indent=2)

        except Exception as e:
            self.log(f"ERROR: Failed to save today's metrics: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _enrich_yesterdays_metrics(self) -> None:
        """Enrich yesterday's metrics with calculated statistics (min, avg, max, std)."""
        try:
            if not self.metrics_file.exists():
                return

            with open(self.metrics_file, 'r') as f:
                data = json.load(f)

            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

            # Find yesterday's entry
            for entry in data.get("daily_metrics", []):
                if entry.get("date") == yesterday:
                    # Enrich notification and delivery times with statistics
                    for metric_type in ["notification", "delivery"]:
                        key = f"{metric_type}_times"
                        if key in entry and entry[key]:
                            times = entry[key]
                            entry[key] = self._create_metrics_dict(times, metric_type, include_raw=True)
                            metrics = entry[key]
                            self.log(
                                f"Enriched yesterday's {metric_type} metrics: {metrics[f'total_{metric_type}']} events, "
                                f"min={metrics[f'min_{metric_type}_seconds']}s, "
                                f"avg={metrics[f'avg_{metric_type}_seconds']}s, "
                                f"max={metrics[f'max_{metric_type}_seconds']}s, "
                                f"std={metrics[f'std_{metric_type}_seconds']}s"
                            )

                    # Save the enriched data back to file
                    with open(self.metrics_file, 'w') as f:
                        json.dump(data, f, indent=2)

                    return

        except Exception as e:
            line_num = sys.exc_info()[2].tb_lineno
            self.log(f"ERROR: Failed to enrich yesterday's metrics: {e} (line {line_num})", level="ERROR")

    def _load_yesterday_metrics(self) -> Optional[Dict[str, Any]]:
        """Load yesterday's metrics from file."""
        try:
            if not self.metrics_file.exists():
                return None

            with open(self.metrics_file, 'r') as f:
                data = json.load(f)

            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            for entry in data.get("daily_metrics", []):
                if entry.get("date") == yesterday:
                    result = {"date": entry["date"]}
                    result.update(self._extract_metrics_from_entry(entry, "notification"))
                    result.update(self._extract_metrics_from_entry(entry, "delivery"))
                    return result

            return None

        except Exception as e:
            line_num = sys.exc_info()[2].tb_lineno
            self.log(f"ERROR: Failed to load yesterday's metrics: {e} (line {line_num})", level="ERROR")
            return None

    def _cleanup_old_files(self, **kwargs) -> None:
        """Clean up old image files to prevent disk space issues."""
        if not self.snapshot_dir or not self.snapshot_dir.exists():
            return

        try:
            cutoff_time = datetime.now() - timedelta(days=self.max_file_age_days)
            files_removed = 0

            # Clean up image files
            for file_path in self.snapshot_dir.rglob("*.jpg"):
                if file_path.stat().st_mtime < cutoff_time.timestamp():
                    file_path.unlink()
                    files_removed += 1

            if files_removed > 0:
                self.log(f"Cleaned up {files_removed} old image files")

        except Exception as e:
            self.log(f"ERROR: Failed to cleanup old files: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _cleanup_cache(self, **kwargs) -> None:
        """Clean up expired cache entries and limit cache size."""
        try:
            cutoff_time = datetime.now() - timedelta(hours=self.cache_ttl_hours)

            with self.cache_lock:
                # Remove expired entries
                expired_keys = [key for key, entry in self.file_cache.items() if entry["timestamp"] < cutoff_time]
                for key in expired_keys:
                    del self.file_cache[key]

                # Limit cache size if needed
                if len(self.file_cache) > 1000:
                    # Sort by timestamp and remove oldest entries
                    entries_to_remove = len(self.file_cache) - 1000
                    sorted_entries = sorted(self.file_cache.items(), key=lambda x: x[1]["timestamp"])
                    for key, _ in sorted_entries[:entries_to_remove]:
                        del self.file_cache[key]

            if expired_keys:
                self.log(f"Cleaned up {len(expired_keys)} expired cache entries")

        except Exception as e:
            self.log(f"ERROR: Failed to cleanup cache: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _cleanup_notified_events(self, **kwargs) -> None:
        """Clean up notified events set to prevent memory leaks."""
        try:
            with self.notification_lock:
                if len(self.notified_events) > 1000:
                    # Keep only the most recent 500 events
                    self.notified_events.clear()
                    self.log("Cleaned up notified events set to prevent memory leaks")
        except Exception as e:
            line_num = sys.exc_info()[2].tb_lineno
            self.log(f"ERROR: Failed to cleanup notified events: {e} (line {line_num})", level="ERROR")

    def _cleanup_circuit_breakers(self, **kwargs) -> None:
        """Clean up old circuit breaker states to prevent memory leaks."""
        try:
            current_time = time.time()
            with self.circuit_breaker_lock:
                # Remove circuit breaker states older than 24 hours
                keys_to_remove = [
                    key for key, state in self.circuit_breaker_state.items()
                    if current_time - state["last_failure"] > 24 * 60 * 60
                ]
                
                for key in keys_to_remove:
                    del self.circuit_breaker_state[key]
                    
                if keys_to_remove:
                    self.log(f"Cleaned up {len(keys_to_remove)} old circuit breaker states")
        except Exception as e:
            line_num = sys.exc_info()[2].tb_lineno
            self.log(f"ERROR: Failed to cleanup circuit breakers: {e} (line {line_num})", level="ERROR")

    def terminate(self) -> None:
        """Cleanup when app is terminated."""
        self.log("Shutting down Frigate Notifier...")

        self.shutdown_event.set()

        if self.processing_thread and self.processing_thread.is_alive():
            self.processing_thread.join(timeout=5)

        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)

        self.log("Frigate Notifier terminated")

