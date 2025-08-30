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
    MIN_FILE_SIZE_BYTES = 50000

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

        # Duplicate notification prevention
        self.notified_events = set()
        self.notification_lock = threading.Lock()

        self._load_config()
        self._setup_mqtt()

        # Start processing thread
        self.processing_thread = threading.Thread(
            target=self._event_processing_worker, name="EventProcessor", daemon=True
        )
        self.processing_thread.start()

        # Schedule periodic tasks
        now = datetime.now()
        self.run_every(self._cleanup_old_files, now, 24 * 60 * 60)
        self.run_every(self._cleanup_cache, now, 6 * 60 * 60)
        self.run_every(self._cleanup_notified_events, now, 60 * 60)

    def _load_config(self) -> None:
        """Load and validate configuration parameters."""
        # Validate and set required parameters
        required_params = ["frigate_url", "ext_domain"]
        for param_name in required_params:
            if not (value := self.args.get(param_name)):
                self.log(f"ERROR: {param_name} is required", level="ERROR")
                raise ValueError(f"Required parameter {param_name} is missing")
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

    def _log_error(self, message: str, exception: Exception) -> None:
        """Log error with line number information."""
        line_num = "unknown"
        try:
            exc_info = sys.exc_info()
            if exc_info[2]:
                line_num = exc_info[2].tb_lineno
        except (AttributeError, TypeError):
            pass
        finally:
            # CRITICAL: Clear exc_info to prevent memory leak
            exc_info = None
        self.log(f"ERROR: {message}: {exception} (line {line_num})", level="ERROR")

    def _cache_file(self, cache_key: str, file_path: Path, timestamp: datetime, file_size: int) -> None:
        """Add file to cache with metadata."""
        with self.cache_lock:
            self.file_cache[cache_key] = {
                "file_path": str(file_path),
                "timestamp": timestamp,
                "size": file_size,
                "checksum": hashlib.md5(f"{cache_key}_{file_size}".encode()).hexdigest()
            }

    def _load_person_configs(self) -> None:
        """Load and validate person notification configurations."""
        for person_data in self.args.get("persons", []):
            try:
                name, notify, labels = person_data.get("name"), person_data.get("notify"), person_data.get("labels", [])
                if not all([name, notify, labels]):
                    self.log(f"ERROR: Missing required fields for person config: name={name}, notify={notify}, labels={labels}", level="ERROR")
                    continue

                self.person_configs.append({
                    "name": name,
                    "notify": notify,
                    "labels": set(labels),
                    "cooldown": max(0, person_data.get("cooldown", 0)),
                    "enabled": person_data.get("enabled", True),
                    "zones": set(zones) if (zones := person_data.get("zones")) else None,
                    "cameras": set(cameras) if (cameras := person_data.get("cameras")) else None
                })

            except Exception as e:
                self._log_error("Failed to load person config", e)

    def _setup_mqtt(self) -> None:
        """Set up MQTT connection and subscribe to Frigate events."""
        try:
            self.mqtt = self.get_plugin_api("MQTT")
            if self.mqtt.is_client_connected():
                self.mqtt.mqtt_subscribe(f"{self.mqtt_topic}/#")
                self.mqtt.listen_event(self._handle_mqtt_message, "MQTT_MESSAGE")
                self.log(f"MQTT setup successful, subscribed to {self.mqtt_topic}/#")
            else:
                self.log("ERROR: MQTT client not connected", level="ERROR")
                raise RuntimeError("MQTT client not connected")
        except Exception as e:
            self._log_error("Failed to set up MQTT", e)
            raise

    def _event_processing_worker(self) -> None:
        """Background worker thread for processing queued events."""
        while not self.shutdown_event.is_set():
            try:
                with self.queue_lock:
                    if self.event_queue:
                        event_data = self.event_queue.popleft()
                    else:
                        event_data = None

                if event_data:
                    try:
                        self.executor.submit(self._download_and_notify, event_data)
                    except Exception as e:
                        self._log_error("Failed to process event", e)
                else:
                    time.sleep(0.1)
            except Exception as e:
                self._log_error("Event processing worker error", e)

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

            # Handle frigate/events messages
            event_data = self._extract_event_data(payload)
            if (not event_data or event_data["event_type"] != "end" or
                event_data.get("false_positive", False) or
                (self.only_zones and not event_data["entered_zones"]) or
                not self._has_potential_recipients(event_data)):
                return

            # Queue event for processing
            with self.queue_lock:
                if len(self.event_queue) < self.MAX_QUEUE_SIZE:
                    self.event_queue.append(event_data)
                else:
                    event_id = event_data["event_id"]
                    self.log(f"Event queue full, dropping event {event_id.split('-')[-1] if '-' in event_id else event_id}")

        except Exception as e:
            self._log_error("Failed to process MQTT message", e)

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
            face_detected = face_confidence = None
            if self.face_detection_enabled and (sub_label := event_data.get("sub_label")):
                if (isinstance(sub_label, list) and len(sub_label) >= 2 and
                    isinstance(sub_label[0], str) and isinstance(sub_label[1], (int, float)) and
                    sub_label[1] >= self.face_detection_threshold):
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
                "top_score": event_data.get("top_score", 0.0),
                "current_zones": event_data.get("current_zones", []),
                "stationary": event_data.get("stationary", False),
                "false_positive": event_data.get("false_positive", False)
            }

        except Exception as e:
            self._log_error("Failed to extract event data", e)
            return None

    def _download_and_notify(self, event_data: Dict[str, Any]) -> None:
        """Download media and send notifications for a Frigate event."""
        try:
            # Try to download snapshot image
            media_path = self._download_media_with_retry(
                event_data["event_id"], event_data["camera"], "snapshot.jpg", ".jpg", 15
            )
            self._send_notifications(event_data, media_path, "image" if media_path else None)
        except Exception as e:
            self._log_error(f"Failed to download and notify for event {event_data['event_id']}", e)

    def _should_notify_user(self, config: Dict[str, Any], event_data: Dict[str, Any], current_time: float) -> bool:
        """Check if a user should receive a notification for this event."""
        if not config["enabled"] or event_data["label"] not in config["labels"]:
            return False

        camera = event_data["camera"]
        if config["cameras"] and camera not in config["cameras"]:
            return False

        # Zone matching using both entered and current zones
        if user_zones := config.get("zones"):
            all_zones = set(event_data["entered_zones"]) | set(event_data.get("current_zones", []))
            if not (user_zones & all_zones):
                return False

        # Check cooldown period
        return current_time - self.msg_cooldown.get(f"{config['notify']}/{camera}", 0) >= config["cooldown"]

    def _has_potential_recipients(self, event_data: Dict[str, Any]) -> bool:
        """Check if any user would receive notifications for this event."""
        if not self.person_configs:
            return False
        current_time = time.time()
        return any(self._should_notify_user(config, event_data, current_time) for config in self.person_configs)

    def _download_media_with_retry(
        self, event_id: str, camera: str, endpoint: str, extension: str, max_timeout: int
    ) -> Optional[str]:
        """Download media with exponential backoff and retry logic."""
        start_time = time.time()
        max_attempts = 3

        for attempt in range(1, max_attempts + 1):
            current_time = time.time()
            if current_time - start_time >= max_timeout:
                break

            try:
                media_path = self._download_media(event_id, camera, endpoint, extension)
                if media_path:
                    return media_path
            except Exception as e:
                self._log_error(f"Media download attempt {attempt} failed for {event_id}", e)

            # Simple exponential backoff for retry
            if attempt < max_attempts:
                delay = min(2 ** attempt, 4)  # 2s, 4s max
                time.sleep(delay)

        return None

    def _download_media(self, event_id: str, camera: str, endpoint: str, extension: str) -> Optional[str]:
        """Download media file from Frigate."""
        if not self.snapshot_dir:
            self.log(f"ERROR: No snapshot directory configured, cannot download {endpoint}", level="ERROR")
            return None

        cache_key = f"{event_id}_{camera}_{endpoint}"

        # Check cache first
        with self.cache_lock:
            cache_entry = self.file_cache.get(cache_key)
            if cache_entry:
                cache_age = (datetime.now() - cache_entry["timestamp"]).total_seconds()
                if cache_age < self.cache_ttl_hours * 3600:
                    # Always return relative path for consistency
                    cached_path = cache_entry["file_path"]
                    if cached_path.startswith(str(self.snapshot_dir)):
                        return str(Path(cached_path).relative_to(self.snapshot_dir))
                    # If cached path is already relative, return as-is
                    return cached_path

        # Download new media
        now = datetime.now()
        date_dir = now.strftime("%Y-%m-%d")
        target_dir = self.snapshot_dir / camera / date_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{now.strftime('%Y%m%d_%H%M%S')}--{event_id}{extension}"
        target_path = target_dir / filename
        relative_path = f"{camera}/{date_dir}/{filename}"

        if target_path.exists():
            self._cache_file(cache_key, target_path, now, target_path.stat().st_size)
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
            self._cache_file(cache_key, target_path, now, file_size)
            return relative_path

        except Exception:
            # Clean up partial file on error
            if target_path.exists():
                target_path.unlink()
            raise

    def _send_notifications(
        self, event_data: Dict[str, Any], media_path: Optional[str], media_type: Optional[str]
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
        camera = event_data["camera"]
        label = event_data["label"]
        timestamp = event_data["timestamp"].strftime("%H:%M:%S")
        zone_str = ", ".join(event_data["entered_zones"]) if event_data["entered_zones"] else "No zones"

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

        if media_path and media_type == "image":
            notification_data["image"] = f"{self.ext_domain}/local/frigate/{media_path.replace('\\', '/').lstrip('/')}"

        # Build notification content
        face_detected = event_data.get("face_detected")
        title = f"{face_detected} ({label}) @ {camera}" if face_detected else f"{label} @ {camera}"
        message = f"{timestamp} - {zone_str} (ID: {event_id})"

        # Send notifications to eligible users
        notifications_sent = 0
        current_time = time.time()

        for config in self.person_configs:
            if not self._should_notify_user(config, event_data, current_time):
                continue

            self.call_service(f"notify/{config['notify']}", title=title, message=message, data=notification_data)
            self.msg_cooldown[f"{config['notify']}/{camera}"] = current_time
            notifications_sent += 1

            # Build log message with available info
            log_parts = [f"Notification sent to {config['name']} - {title} - Event ID: {event_id}"]
            if media_path:
                log_parts.append(f"{media_type}: {media_path}")
            if face_detected:
                face_part = f"Face: {face_detected}"
                if face_confidence := event_data.get('face_confidence'):
                    face_part += f" (confidence: {face_confidence:.2f})"
                log_parts.append(face_part)
            self.log(" - ".join(log_parts))

        if notifications_sent > 0:
            notification_time = time.time() - notification_start
            self.log(f"Sent {notifications_sent} notifications for {event_id} in {notification_time:.3f}s")

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
            self._log_error("Failed to cleanup old files", e)

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
                    oldest_entries = sorted(self.file_cache.items(), key=lambda x: x[1]["timestamp"])
                    for key, _ in oldest_entries[:entries_to_remove]:
                        del self.file_cache[key]

            if expired_keys:
                self.log(f"Cleaned up {len(expired_keys)} expired cache entries")
        except Exception as e:
            self._log_error("Failed to cleanup cache", e)

    def _cleanup_notified_events(self, **kwargs) -> None:
        """Clean up notified events set to prevent memory leaks."""
        try:
            # CRITICAL: Clean up cooldown dictionary to prevent memory leak
            current_time = time.time()
            if self.person_configs:
                max_cooldown = max(config.get("cooldown", 0) for config in self.person_configs)
            else:
                max_cooldown = 3600  # Default 1 hour when no person configs
            cutoff_time = current_time - (max_cooldown * 2)  # Keep 2x max cooldown for safety

            with self.notification_lock:
                if len(self.notified_events) > self.MAX_NOTIFIED_EVENTS:
                    self.notified_events.clear()
                    self.log("Cleaned up notified events set to prevent memory leaks")

                # Clean up cooldown dictionary (must be inside same lock as notifications)
                keys_to_remove = [key for key, timestamp in self.msg_cooldown.items() if timestamp < cutoff_time]
                for key in keys_to_remove:
                    del self.msg_cooldown[key]

                if keys_to_remove:
                    self.log(f"Cleaned up {len(keys_to_remove)} expired cooldown entries to prevent memory leak")

        except Exception as e:
            self._log_error("Failed to cleanup notified events", e)
