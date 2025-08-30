"""
Frigate Notification App for AppDaemon

Copyright (c) 2025 the_louie
All rights reserved.

This app listens to Frigate MQTT events and sends notifications to configured users
when motion is detected. It supports zone filtering, cooldown periods, snapshot
image downloads, and face detection. Notifications include image attachments and
action links to view the corresponding video clips. When face detection is enabled
and a known person is recognized, their name will be included in the notification title.

Configuration:
  frigate_notify:
    module: i1_frigate_notifier
    class: FrigateNotification
    mqtt_topic: "frigate/events"
    frigate_url: "https://frigate.example.com/api/events"
    ext_domain: "https://your-domain.com"
    snapshot_dir: "/path/to/snapshots"
    only_zones: true
    face_detection_enabled: true
    face_detection_threshold: 0.7
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
from typing import Any, Dict, Optional

import appdaemon.plugins.hass.hassapi as hass


class FrigateNotification(hass.Hass):
    """AppDaemon app for sending Frigate motion notifications."""

    # Constants
    MAX_QUEUE_SIZE = 1000
    MAX_CACHE_SIZE = 1000
    MAX_NOTIFIED_EVENTS = 1000
    CIRCUIT_BREAKER_TIMEOUT = 60
    CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5
    MIN_FILE_SIZE_BYTES = 50000
    DELIVERY_TIME_THRESHOLD = 3600

    def initialize(self) -> None:
        """Initialize the Frigate notification app."""
        self.msg_cooldown = {}
        self.person_configs = []
        self.event_queue = deque(maxlen=self.MAX_QUEUE_SIZE)
        self.queue_lock = threading.Lock()
        self.processing_thread = None
        self.shutdown_event = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="FrigateNotifier")
        self.file_cache = {}
        self.cache_lock = threading.Lock()

        # Metrics tracking
        self.notification_times = []
        self.delivery_times = []
        self.metrics_lock = threading.Lock()
        self.metrics_file = Path(__file__).parent / "notification_metrics.json"

        # Duplicate notification prevention
        self.notified_events = set()  # Track events that have already been notified
        self.notification_lock = threading.Lock()

        # Circuit breaker for download failures
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
        """Load and validate configuration parameters."""
        # Validate required parameters
        for param_name in ["frigate_url", "ext_domain"]:
            value = self.args.get(param_name)
            if not value:
                self.log(f"ERROR: {param_name} is required", level="ERROR")
                return
            setattr(self, param_name, value)

        # Load optional parameters with defaults
        self.mqtt_topic = self.args.get("mqtt_topic", "frigate/events")
        self.only_zones = self.args.get("only_zones", False)
        self.cam_icons = self.args.get("cam_icons", {})
        self.max_file_age_days = self.args.get("max_file_age_days", 30)
        self.cache_ttl_hours = self.args.get("cache_ttl_hours", 24)
        self.connection_timeout = self.args.get("connection_timeout", 30)

        # Face detection configuration
        self.face_detection_enabled = self.args.get("face_detection_enabled", True)
        threshold = self.args.get("face_detection_threshold", 0.7)
        self.face_detection_threshold = max(0.0, min(1.0, threshold))  # Ensure 0.0-1.0 range

        # Configure snapshot directory
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
                name = person_data.get("name")
                notify = person_data.get("notify")
                labels = person_data.get("labels", [])

                if not all([name, notify, labels]):
                    self.log(f"ERROR: Missing required fields for person config: name={name}, notify={notify}, labels={labels}", level="ERROR")
                    continue

                zones = person_data.get("zones")
                cameras = person_data.get("cameras")
                config = {
                    "name": name,
                    "notify": notify,
                    "labels": set(labels),
                    "cooldown": max(0, person_data.get("cooldown", 0)),
                    "enabled": person_data.get("enabled", True),
                    "zones": set(zones) if zones else None,
                    "cameras": set(cameras) if cameras else None
                }
                self.person_configs.append(config)

            except Exception as e:
                line_num = sys.exc_info()[2].tb_lineno
                self.log(f"ERROR: Failed to load person config: {e} (line {line_num})", level="ERROR")

    def _setup_mqtt(self) -> None:
        """Set up MQTT connection and subscribe to Frigate events."""
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
        """Background worker thread for processing queued events."""
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
            if (not data or 'topic' not in data or 'payload' not in data or
                not data['topic'].startswith(self.mqtt_topic)):
                return

            topic = data['topic']
            if not topic.endswith('/events'):
                return

            payload = data['payload']
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    self.log("ERROR: Invalid JSON payload", level="ERROR")
                    return

            self._handle_events_message(payload)

        except Exception as e:
            line_num = sys.exc_info()[2].tb_lineno
            self.log(f"ERROR: Failed to process MQTT message: {e} (line {line_num})", level="ERROR")

    def _handle_events_message(self, payload: Dict[str, Any]) -> None:
        """Handle frigate/events messages."""
        event_data = self._extract_event_data(payload)
        if (not event_data or event_data["event_type"] != "end" or
            event_data.get("false_positive", False) or
            not self._has_potential_recipients(event_data)):
            return

        # Queue event for processing
        with self.queue_lock:
            if len(self.event_queue) < self.MAX_QUEUE_SIZE:
                self.event_queue.append(event_data)
            else:
                event_id = event_data["event_id"]
                event_suffix = event_id.split('-')[-1] if '-' in event_id else event_id
                self.log(f"Event queue full, dropping event {event_suffix}")



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
                if 0 <= delivery_time <= self.DELIVERY_TIME_THRESHOLD:
                    with self.metrics_lock:
                        self.delivery_times.append(delivery_time)
                        # Save metrics immediately after each delivery time
                        self._save_todays_metrics()

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

            # Extract face detection data - sub_label is an array: [name, confidence]
            sub_label = event_data.get("sub_label")
            top_score = event_data.get("top_score", 0.0)
            face_detected = None
            face_confidence = None

            if (self.face_detection_enabled and sub_label and isinstance(sub_label, list) and
                len(sub_label) >= 2 and isinstance(sub_label[0], str) and
                isinstance(sub_label[1], (int, float)) and sub_label[1] >= self.face_detection_threshold):
                face_detected = sub_label[0].strip()
                face_confidence = sub_label[1]

            return {
                "event_id": event_id,
                "camera": camera,
                "label": label,
                "entered_zones": entered_zones,
                "event_type": payload.get("type", ""),
                "timestamp": datetime.now(),
                "face_detected": face_detected,
                "face_confidence": face_confidence,
                "top_score": top_score,
                "current_zones": event_data.get("current_zones", []),
                "stationary": event_data.get("stationary", False),
                "false_positive": event_data.get("false_positive", False)
            }

        except Exception as e:
            self.log(f"ERROR: Failed to extract event data: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")
            return None

    def _process_event(self, event_data: Dict[str, Any]) -> None:
        """Process a Frigate event."""
        try:
            if self.only_zones and not event_data["entered_zones"]:
                return
            self.executor.submit(self._download_and_notify, event_data)
        except Exception as e:
            line_num = sys.exc_info()[2].tb_lineno
            self.log(f"ERROR: Failed to process event: {e} (line {line_num})", level="ERROR")

    def _download_and_notify(self, event_data: Dict[str, Any]) -> None:
        """Download media and send notifications for a Frigate event."""
        event_id = event_data["event_id"]
        event_suffix = event_id.split('-')[-1] if '-' in event_id else event_id

        try:
            # Try to download snapshot image with intelligent retry
            media_path = self._download_media_with_retry(
                event_data["event_id"], event_data["camera"], "snapshot.jpg", ".jpg", 15, event_suffix
            )
            if media_path:
                self._send_notifications(event_data, media_path, "image", event_suffix)
                return

            # Send notification without media if snapshot download failed
            self._send_notifications(event_data, None, None, event_suffix)

        except Exception as e:
            line_num = sys.exc_info()[2].tb_lineno
            self.log(f"ERROR: Failed to download and notify for event {event_id}: {e} (line {line_num})", level="ERROR")

    def _should_notify_user(self, config: Dict[str, Any], event_data: Dict[str, Any], current_time: float) -> bool:
        """Check if a user should receive a notification for this event."""
        if not config["enabled"] or event_data["label"] not in config["labels"]:
            return False

        camera = event_data["camera"]
        if config["cameras"] and camera not in config["cameras"]:
            return False

        # Zone matching using both entered and current zones
        user_zones = config.get("zones")
        if user_zones:
            all_zones = set(event_data["entered_zones"]) | set(event_data.get("current_zones", []))
            if not (user_zones & all_zones):
                return False

        # Check cooldown period
        cooldown_key = f"{config['notify']}/{camera}"
        return current_time - self.msg_cooldown.get(cooldown_key, 0) >= config["cooldown"]

    def _has_potential_recipients(self, event_data: Dict[str, Any]) -> bool:
        """Check if any user would receive notifications for this event."""
        if not self.person_configs:
            return False

        current_time = time.time()
        return any(self._should_notify_user(config, event_data, current_time) for config in self.person_configs)

    def _is_circuit_closed(self, circuit_key: str) -> bool:
        """Check if circuit breaker is closed (allowing requests)."""
        with self.circuit_breaker_lock:
            state_info = self.circuit_breaker_state.get(circuit_key)
            if not state_info:
                return True

            state = state_info["state"]
            if state == "closed":
                return True

            current_time = time.time()
            if state == "open" and current_time - state_info["last_failure"] > self.CIRCUIT_BREAKER_TIMEOUT:
                state_info["state"] = "half_open"
                return True
            elif state == "half_open":
                state_info["state"] = "open"  # Block further requests until success/failure
                return True

            return state != "open"

    def _record_circuit_failure(self, circuit_key: str) -> None:
        """Record a failure for circuit breaker tracking."""
        with self.circuit_breaker_lock:
            state_info = self.circuit_breaker_state.setdefault(circuit_key, {
                "failures": 0, "last_failure": 0, "state": "closed"
            })

            state_info["failures"] += 1
            state_info["last_failure"] = time.time()

            # Open circuit if too many failures
            if state_info["failures"] >= self.CIRCUIT_BREAKER_FAILURE_THRESHOLD:
                state_info["state"] = "open"
                self.log(f"Circuit breaker OPENED for {circuit_key} due to {state_info['failures']} failures")

    def _reset_circuit_breaker(self, circuit_key: str) -> None:
        """Reset circuit breaker after successful operation."""
        with self.circuit_breaker_lock:
            state_info = self.circuit_breaker_state.get(circuit_key)
            if state_info:
                state_info.update({"failures": 0, "last_failure": 0, "state": "closed"})

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
        max_timeout: int, event_suffix: str
    ) -> Optional[str]:
        """Download media with exponential backoff, jitter, and circuit breaker."""
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
                else:
                    # No exception but no media path - record as failure
                    self._record_circuit_failure(circuit_key)

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
                base_delay = min(2 ** attempt, 8)  # 1s, 2s, 4s, 8s max
                jitter = random.uniform(-0.25, 0.25) * base_delay  # ±25% jitter
                delay = max(0.1, base_delay + jitter)  # Minimum 0.1s delay
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
            cache_entry = self.file_cache.get(cache_key)
            if cache_entry:
                cache_age = (datetime.now() - cache_entry["timestamp"]).total_seconds()
                if cache_age < self.cache_ttl_hours * 3600:
                    # Return relative path for consistency
                    cached_path = cache_entry["file_path"]
                    if cached_path.startswith(str(self.snapshot_dir)):
                        return str(Path(cached_path).relative_to(self.snapshot_dir))
                    return cached_path

        # Download new media
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H:%M:%S")
        date_dir = now.strftime("%Y-%m-%d")
        target_dir = self.snapshot_dir / camera / date_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{timestamp}--{event_id}{extension}"
        target_path = target_dir / filename
        relative_path = f"{camera}/{date_dir}/{filename}"

        if target_path.exists():
            # Add existing file to cache
            file_size = target_path.stat().st_size
            with self.cache_lock:
                self.file_cache[cache_key] = {
                    "file_path": str(target_path),
                    "timestamp": now,
                    "size": file_size,
                    "checksum": hashlib.md5(f"{cache_key}_{file_size}".encode()).hexdigest()
                }
            return relative_path

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
                if file_size < self.MIN_FILE_SIZE_BYTES:
                    raise ValueError(f"File too small: {file_size} bytes (minimum {self.MIN_FILE_SIZE_BYTES})")

                # Write file to disk
                with open(target_path, 'wb') as f:
                    f.write(content)

            # Cache the downloaded file
            with self.cache_lock:
                self.file_cache[cache_key] = {
                    "file_path": str(target_path),
                    "timestamp": now,
                    "size": file_size,
                    "checksum": hashlib.md5(f"{cache_key}_{file_size}".encode()).hexdigest()
                }
            return relative_path

        except Exception:
            # Clean up partial file on error
            if target_path.exists():
                target_path.unlink()
            raise

    def _send_notifications(
        self, event_data: Dict[str, Any], media_path: Optional[str],
        media_type: Optional[str], event_suffix: str
    ) -> None:
        """Send notifications to all eligible recipients."""
        if not self.person_configs:
            return

        event_id = event_data["event_id"]

        # Check if already notified to prevent duplicates
        with self.notification_lock:
            if event_id in self.notified_events:
                return
            self.notified_events.add(event_id)

        notification_start = time.time()
        timestamp = event_data["timestamp"].strftime("%H:%M:%S")
        camera = event_data["camera"]
        label = event_data["label"]
        entered_zones = event_data["entered_zones"]
        zone_str = ", ".join(entered_zones) if entered_zones else "No zones"

        # Build notification data
        notification_data = {
            "actions": [
                {"action": "URI", "title": "Open Camera", "uri": f"homeassistant://navigate/dashboard-kameror/{camera}"},
                {"action": "URI", "title": "Video", "uri": f"{self.ext_domain}/api/frigate/frigate/notifications/{event_id}/clip.mp4"}
            ],
            "channel": f"frigate-{camera}",
            "importance": "high",
            "visibility": "public",
            "priority": "high",
            "ttl": 0,
            "event_id": event_id,
            "timestamp": timestamp,
            "notification_icon": self.cam_icons.get(camera, "mdi:cctv"),
            "confirmation": True
        }

        # Add image to notification if available
        if media_path and self.ext_domain and media_type == "image":
            media_url = f"{self.ext_domain}/local/frigate/{media_path.lstrip('/')}"
            notification_data["image"] = media_url

        # Send notifications to eligible users
        notifications_sent = 0
        current_time = time.time()

        # Build notification content
        face_detected = event_data.get("face_detected")
        title = f"{face_detected} ({label}) @ {camera}" if face_detected else f"{label} @ {camera}"
        message = f"{timestamp} - {zone_str} (ID: {event_id})"

        # Prepare logging info
        media_info = f" - {media_type}: {media_path}" if media_path else " - no media"
        face_info = f" - Face: {face_detected}" if face_detected else ""

        for config in self.person_configs:
            if not self._should_notify_user(config, event_data, current_time):
                continue

            self.call_service(f"notify/{config['notify']}", title=title, message=message, data=notification_data)
            self.msg_cooldown[f"{config['notify']}/{camera}"] = current_time
            notifications_sent += 1

            # Log with face confidence if available
            face_confidence = event_data.get('face_confidence')
            face_confidence_info = f" (confidence: {face_confidence:.2f})" if face_detected and face_confidence else ""
            self.log(f"Notification sent to {config['name']} - {title} - Event ID: {event_id}{media_info}{face_info}{face_confidence_info}")

        # Record metrics and log completion
        notification_time = time.time() - notification_start
        with self.metrics_lock:
            self.notification_times.append(notification_time)

        if notifications_sent > 0:
            self.log(f"Sent {notifications_sent} notifications for {event_id} in {notification_time:.3f}s")

    def _log_daily_metrics(self, **kwargs) -> None:
        """Log daily notification performance metrics and enrich previous day's data."""
        try:
            with self.metrics_lock:
                if not self.notification_times and not self.delivery_times:
                    self.log("METRICS: No notifications sent today")
                    return

                log_parts = ["METRICS:"]

                for times, label in [(self.notification_times, "Notifications"), (self.delivery_times, "Deliveries")]:
                    if times:
                        count = len(times)
                        avg_time = sum(times) / count
                        min_time = min(times)
                        max_time = max(times)
                        log_parts.append(f"{label}={count}, Min={min_time:.3f}s, Avg={avg_time:.3f}s, Max={max_time:.3f}s")

                self.log(" | ".join(log_parts))

                # Clear today's data for next day
                self.notification_times.clear()
                self.delivery_times.clear()

        except Exception as e:
            self.log(f"ERROR: Failed to log daily metrics: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _load_todays_metrics(self) -> None:
        """Load today's metrics from file on startup."""
        try:
            if not self.metrics_file.exists():
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

                    return

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

    def _cleanup_old_files(self, **kwargs) -> None:
        """Clean up old image files to prevent disk space issues."""
        if not self.snapshot_dir or not self.snapshot_dir.exists():
            return

        try:
            cutoff_time = datetime.now() - timedelta(days=self.max_file_age_days)
            files_removed = 0

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
                expired_keys = [key for key, entry in self.file_cache.items() if entry["timestamp"] < cutoff_time]
                for key in expired_keys:
                    del self.file_cache[key]

                if len(self.file_cache) > self.MAX_CACHE_SIZE:
                    entries_to_remove = len(self.file_cache) - self.MAX_CACHE_SIZE
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
                if len(self.notified_events) > self.MAX_NOTIFIED_EVENTS:
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
                keys_to_remove = [key for key, state in self.circuit_breaker_state.items()
                                 if current_time - state["last_failure"] > 86400]  # 24 hours

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

