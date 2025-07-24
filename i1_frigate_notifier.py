"""
Frigate Notification App for AppDaemon

Copyright (c) 2025 the_louie
All rights reserved.

This app listens to Frigate MQTT events and sends notifications to configured users
when motion is detected. It supports zone filtering, cooldown periods, and video clip
downloads.

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

import appdaemon.plugins.hass.hassapi as hass
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


class FrigateNotification(hass.Hass):
    """AppDaemon app for sending Frigate motion notifications."""

    def _get_event_suffix(self, event_id: str) -> str:
        """Get event suffix from event ID."""
        return event_id.split('-')[-1] if '-' in event_id else event_id

    def _create_cache_entry(self, cache_key: str, file_path: Path, file_size: int) -> Dict[str, Any]:
        """Create a cache entry for a file."""
        return {
            "file_path": str(file_path),
            "timestamp": datetime.now(),
            "size": file_size,
            "checksum": hashlib.md5(f"{cache_key}_{file_size}".encode()).hexdigest()
        }

    def _extract_metrics_from_entry(self, entry: Dict[str, Any], metric_type: str) -> Dict[str, Any]:
        """Extract metrics from entry in either old or new format."""
        result = {}
        key = f"{metric_type}_times"

        if key in entry:
            if isinstance(entry[key], dict):
                # New format with raw data and stats
                result.update({
                    f"total_{metric_type}": entry[key].get(f"total_{metric_type}", 0),
                    f"min_{metric_type}_seconds": entry[key].get(f"min_{metric_type}_seconds", 0),
                    f"avg_{metric_type}_seconds": entry[key].get(f"avg_{metric_type}_seconds", 0),
                    f"max_{metric_type}_seconds": entry[key].get(f"max_{metric_type}_seconds", 0),
                    f"std_{metric_type}_seconds": entry[key].get(f"std_{metric_type}_seconds", 0)
                })
            else:
                # Old format - calculate stats
                times = entry[key]
                if times:
                    result.update({
                        f"total_{metric_type}": len(times),
                        f"min_{metric_type}_seconds": round(min(times), 3),
                        f"avg_{metric_type}_seconds": round(sum(times) / len(times), 3),
                        f"max_{metric_type}_seconds": round(max(times), 3),
                        f"std_{metric_type}_seconds": round(self._calculate_std(times), 3)
                    })

        return result

    def _create_enriched_metrics(self, times: list, metric_type: str) -> Dict[str, Any]:
        """Create enriched metrics structure with raw data and calculated statistics."""
        if not times:
            return {}

        return {
            "raw": times,
            f"total_{metric_type}": len(times),
            f"min_{metric_type}_seconds": round(min(times), 3),
            f"avg_{metric_type}_seconds": round(sum(times) / len(times), 3),
            f"max_{metric_type}_seconds": round(max(times), 3),
            f"std_{metric_type}_seconds": round(self._calculate_std(times), 3)
        }

    def initialize(self) -> None:
        """Initialize the app and set up MQTT listener."""
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

        # Load today's metrics on startup
        self._load_todays_metrics()

        # Event tracking for early notification
        self.tracked_events = {}  # event_id -> {start_time, last_check, handled}
        self.event_tracking_lock = threading.Lock()

        self._load_config()
        self._setup_mqtt()

        # Set up notification delivery tracking
        self.listen_event(self._handle_notification_received, "mobile_app_notification_received")

        # Start processing thread
        self.processing_thread = threading.Thread(target=self._event_processing_worker, name="EventProcessor", daemon=True)
        self.processing_thread.start()

        # Start event tracking thread for early clip checking
        self.event_tracking_thread = threading.Thread(target=self._event_tracking_worker, name="EventTracker", daemon=True)
        self.event_tracking_thread.start()

        # Schedule periodic tasks
        self.run_every(self._cleanup_old_files, datetime.now(), 24 * 60 * 60)
        self.run_every(self._log_daily_metrics, datetime.now().replace(hour=23, minute=59, second=0, microsecond=0), 24 * 60 * 60)
        self.run_every(self._cleanup_cache, datetime.now(), 6 * 60 * 60)
        self.run_every(self._cleanup_old_tracked_events, datetime.now(), 5 * 60)  # Every 5 minutes

    def _load_config(self) -> None:
        """Load and validate configuration from args."""
        # Early validation of required parameters
        self.frigate_url = self.args.get("frigate_url")
        if not self.frigate_url:
            self.log("ERROR: frigate_url is required", level="ERROR")
            return

        self.ext_domain = self.args.get("ext_domain")
        if not self.ext_domain:
            self.log("ERROR: ext_domain is required", level="ERROR")
            return

        # Load optional parameters
        self.mqtt_topic = self.args.get("mqtt_topic", "frigate/events")
        self.snapshot_dir = self.args.get("snapshot_dir")
        self.only_zones = self.args.get("only_zones", False)
        self.cam_icons = self.args.get("cam_icons", {})
        self.max_file_age_days = self.args.get("max_file_age_days", 30)
        self.cache_ttl_hours = self.args.get("cache_ttl_hours", 24)
        self.connection_timeout = self.args.get("connection_timeout", 30)

        # Setup snapshot directory if specified
        if self.snapshot_dir:
            self.snapshot_dir = Path(self.snapshot_dir)
            if not self.snapshot_dir.exists():
                self.snapshot_dir.mkdir(parents=True, exist_ok=True)

        self._load_person_configs()

    def _load_person_configs(self) -> None:
        """Load and validate person configurations."""
        persons_raw = self.args.get("persons", [])
        if not persons_raw:
            return

        for person_data in persons_raw:
            try:
                name = person_data.get("name")
                notify = person_data.get("notify")
                labels = person_data.get("labels", [])
                cooldown = person_data.get("cooldown", 0)
                enabled = person_data.get("enabled", True)

                if not all([name, notify, labels]):
                    continue

                self.person_configs.append({
                    "name": name,
                    "notify": notify,
                    "labels": set(labels),
                    "cooldown": cooldown,
                    "zones": set(person_data.get("zones", [])) if person_data.get("zones") else None,
                    "cameras": set(person_data.get("cameras", [])) if person_data.get("cameras") else None,
                    "enabled": enabled
                })

            except Exception as e:
                self.log(f"ERROR: Failed to load person config: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _setup_mqtt(self) -> None:
        """Set up MQTT connection and event listener."""
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
        """Worker thread for processing events from the queue."""
        while not self.shutdown_event.is_set():
            try:
                with self.queue_lock:
                    if self.event_queue:
                        self._process_event(self.event_queue.popleft())
                    else:
                        time.sleep(0.1)
            except Exception as e:
                self.log(f"ERROR: Event processing worker error: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _event_tracking_worker(self) -> None:
        """Worker thread for tracking event ages and checking for early clip availability."""
        while not self.shutdown_event.is_set():
            try:
                current_time = time.time()
                events_to_check = []

                # Get events that need checking
                with self.event_tracking_lock:
                    tracked_count = len(self.tracked_events)
                    for event_id, event_data in self.tracked_events.items():
                        if event_data["handled"]:
                            continue

                        age = current_time - event_data["start_time"]
                        if age >= 35 and (current_time - event_data["last_check"]) >= 5:
                            # Make a copy of event data to avoid race conditions
                            event_copy = {
                                "start_time": event_data["start_time"],
                                "camera": event_data["camera"],
                                "label": event_data["label"],
                                "entered_zones": event_data["entered_zones"].copy() if event_data["entered_zones"] else []
                            }
                            events_to_check.append((event_id, event_copy))
                            event_data["last_check"] = current_time

                # Log tracking status periodically
                if tracked_count > 0 and len(events_to_check) > 0:
                    self.log(f"DEBUG: Tracking {tracked_count} events, checking {len(events_to_check)} for clip availability")

                # Check each event for clip availability
                for event_id, event_data in events_to_check:
                    self._check_event_clip_availability(event_id, event_data)

                time.sleep(1)  # Check every second

            except Exception as e:
                self.log(f"ERROR: Event tracking worker error: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _check_event_clip_availability(self, event_id: str, event_data: Dict[str, Any]) -> None:
        """Check if a clip.mp4 is available for an event and trigger notification if so."""
        event_suffix = self._get_event_suffix(event_id)
        age = time.time() - event_data["start_time"]

        self.log(f"DEBUG[{event_suffix}]: Checking clip availability for event (age: {age:.1f}s)")

        try:
            # Try to download the clip with a single attempt
            media_path = self._download_media(event_id, event_data["camera"], "clip.mp4", ".mp4", event_suffix)

            if media_path:
                self.log(f"DEBUG[{event_suffix}]: Clip available! Triggering early notification")

                # Mark as handled
                with self.event_tracking_lock:
                    event_data["handled"] = True

                # Create event data and send notification
                event_data_for_notification = {
                    "event_id": event_id,
                    "camera": event_data["camera"],
                    "label": event_data["label"],
                    "entered_zones": event_data["entered_zones"],
                    "event_type": "end",
                    "timestamp": datetime.fromtimestamp(event_data["start_time"])
                }

                # Send notification with video
                self._send_notifications(event_data_for_notification, media_path, "video", event_suffix)

            else:
                self.log(f"DEBUG[{event_suffix}]: Clip not yet available, will check again in 5s")

        except Exception as e:
            self.log(f"ERROR: Failed to check clip availability for event {event_id}: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _handle_mqtt_message(self, event_name: str, data: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        """Handle incoming MQTT messages from Frigate."""
        try:
            # Early validation
            if not data or 'topic' not in data or 'payload' not in data:
                return

            topic = data['topic']
            if not topic.startswith(self.mqtt_topic):
                return

            payload = data['payload']
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    self.log("ERROR: Invalid JSON payload", level="ERROR")
                    return

            event_data = self._extract_event_data(payload)
            if not event_data:
                return

            event_id = event_data["event_id"]
            event_suffix = self._get_event_suffix(event_id)
            event_type = event_data["event_type"]

            self.log(f"DEBUG[{event_suffix}]: MQTT event received - Topic: {topic}, Event ID: {event_id}, Type: {event_type}")

            # Track all events for age monitoring
            with self.event_tracking_lock:
                if event_id not in self.tracked_events:
                    # New event - start tracking
                    self.tracked_events[event_id] = {
                        "start_time": time.time(),
                        "last_check": 0,
                        "handled": False,
                        "camera": event_data["camera"],
                        "label": event_data["label"],
                        "entered_zones": event_data["entered_zones"]
                    }
                    self.log(f"DEBUG[{event_suffix}]: Started tracking event (type: {event_type})")
                else:
                    # Update existing event data
                    self.tracked_events[event_id].update({
                        "camera": event_data["camera"],
                        "label": event_data["label"],
                        "entered_zones": event_data["entered_zones"]
                    })

                # Check if event is already handled
                if self.tracked_events[event_id]["handled"]:
                    self.log(f"DEBUG[{event_suffix}]: Event already handled, skipping")
                    return

            # For end events, process immediately
            if event_type == "end":
                self.log(f"DEBUG[{event_suffix}]: End event received, processing immediately")
                # Mark as handled
                with self.event_tracking_lock:
                    self.tracked_events[event_id]["handled"] = True

                # Add to queue for processing
                with self.queue_lock:
                    if len(self.event_queue) < 1000:
                        self.event_queue.append(event_data)
                        self.log(f"DEBUG[{event_suffix}]: End event added to queue (queue size: {len(self.event_queue)})")
                    else:
                        self.log(f"DEBUG[{event_suffix}]: Event queue full, dropping end event")

        except Exception as e:
            self.log(f"ERROR: Failed to process MQTT message: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

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

                    self.log(f"Notification delivered in {delivery_time:.3f}s - Event ID: {notification_data.get('event_id', 'unknown')}")

            except ValueError as e:
                self.log(f"ERROR: Failed to parse notification timestamp: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

        except Exception as e:
            self.log(f"ERROR: Failed to process notification received event: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")



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
        event_suffix = self._get_event_suffix(event_id)

        try:
            self.log(f"DEBUG[{event_suffix}]: Processing event - ID: {event_id}, Type: {event_data['event_type']}, Camera: {event_data['camera']}, Label: {event_data['label']}")

            if event_data["event_type"] != "end":
                self.log(f"DEBUG[{event_suffix}]: Skipping event - not 'end' type (got: {event_data['event_type']})")
                return

            if self.only_zones and not event_data["entered_zones"]:
                self.log(f"DEBUG[{event_suffix}]: Skipping event - only_zones enabled but no zones entered")
                return

            self.log(f"DEBUG[{event_suffix}]: Event passed validation, submitting to thread pool for media download")
            # Submit download and notification work to thread pool
            self.executor.submit(self._download_and_notify, event_data)

        except Exception as e:
            self.log(f"ERROR: Failed to process event: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _download_and_notify(self, event_data: Dict[str, Any]) -> None:
        """Download media and send notifications."""
        event_id = event_data["event_id"]
        event_suffix = self._get_event_suffix(event_id)

        try:
            self.log(f"DEBUG[{event_suffix}]: Starting media download and notification process")

            # Try video first with 30s timeout and 3s retries
            self.log(f"DEBUG[{event_suffix}]: Attempting video download (clip.mp4)")
            media_path = self._download_media_with_retry(event_data["event_id"], event_data["camera"], "clip.mp4", ".mp4", 30, 3, event_suffix)
            if media_path:
                self.log(f"DEBUG[{event_suffix}]: Video download successful: {media_path}")
                self._send_notifications(event_data, media_path, "video", event_suffix)
                return

            # If video fails, try snapshot with 15s timeout and 2s retries
            self.log(f"DEBUG[{event_suffix}]: Video failed, attempting snapshot download (snapshot.jpg)")
            media_path = self._download_media_with_retry(event_data["event_id"], event_data["camera"], "snapshot.jpg", ".jpg", 15, 2, event_suffix)
            if media_path:
                self.log(f"DEBUG[{event_suffix}]: Snapshot download successful: {media_path}")
                self._send_notifications(event_data, media_path, "image", event_suffix)
                return

            # Send notification without media if both downloads failed
            self.log(f"DEBUG[{event_suffix}]: Both video and snapshot downloads failed, sending notification without media")
            self._send_notifications(event_data, None, None, event_suffix)

        except Exception as e:
            self.log(f"ERROR: Failed to download and notify for event {event_id}: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _download_media_with_retry(self, event_id: str, camera: str, endpoint: str, extension: str, timeout: int, retry_interval: int, event_suffix: str) -> Optional[str]:
        """Download media with configurable timeout and retry interval."""
        start_time = time.time()
        attempt = 0

        self.log(f"DEBUG[{event_suffix}]: Starting {endpoint} download with {timeout}s timeout, {retry_interval}s retry interval")

        while time.time() - start_time < timeout:
            attempt += 1
            elapsed = time.time() - start_time

            try:
                self.log(f"DEBUG[{event_suffix}]: {endpoint} download attempt {attempt} (elapsed: {elapsed:.1f}s)")
                media_path = self._download_media(event_id, camera, endpoint, extension, event_suffix)
                if media_path:
                    self.log(f"DEBUG[{event_suffix}]: {endpoint} download successful on attempt {attempt} after {elapsed:.1f}s")
                    return media_path
                else:
                    self.log(f"DEBUG[{event_suffix}]: {endpoint} download returned None on attempt {attempt}")
            except Exception as e:
                self.log(f"ERROR: Media download attempt failed for event {event_id} ({endpoint}): {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

            if time.time() - start_time < timeout:
                self.log(f"DEBUG[{event_suffix}]: Waiting {retry_interval}s before next {endpoint} attempt")
                time.sleep(retry_interval)

        self.log(f"DEBUG[{event_suffix}]: {endpoint} download timeout after {timeout}s ({attempt} attempts)")
        return None

    def _download_media(self, event_id: str, camera: str, endpoint: str, extension: str, event_suffix: str) -> Optional[str]:
        """Download media file from Frigate."""
        cache_key = f"{event_id}_{camera}_{endpoint}"

        self.log(f"DEBUG[{event_suffix}]: Checking cache for {endpoint} (key: {cache_key})")

        # Check cache first
        with self.cache_lock:
            if cache_key in self.file_cache:
                cache_entry = self.file_cache[cache_key]
                cache_age = (datetime.now() - cache_entry["timestamp"]).total_seconds()
                if cache_age < self.cache_ttl_hours * 3600:
                    self.log(f"DEBUG[{event_suffix}]: {endpoint} found in cache (age: {cache_age:.1f}s), returning: {cache_entry['file_path']}")
                    return cache_entry["file_path"]
                else:
                    self.log(f"DEBUG[{event_suffix}]: {endpoint} cache entry expired (age: {cache_age:.1f}s)")
            else:
                self.log(f"DEBUG[{event_suffix}]: {endpoint} not found in cache")

        # Download new media
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H:%M:%S")
        date_dir = now.strftime("%Y-%m-%d")
        target_dir = self.snapshot_dir / camera / date_dir

        self.log(f"DEBUG[{event_suffix}]: Creating target directory: {target_dir}")
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{timestamp}--{event_id}{extension}"
        target_path = target_dir / filename

        self.log(f"DEBUG[{event_suffix}]: Target file path: {target_path}")

        if target_path.exists():
            self.log(f"DEBUG[{event_suffix}]: {endpoint} file already exists, adding to cache")
            with self.cache_lock:
                file_size = target_path.stat().st_size
                self.file_cache[cache_key] = self._create_cache_entry(cache_key, target_path, file_size)
            relative_path = f"{camera}/{date_dir}/{filename}"
            self.log(f"DEBUG[{event_suffix}]: Returning existing {endpoint} file: {relative_path}")
            return relative_path

        media_url = f"{self.frigate_url}/{event_id}/{endpoint}"
        self.log(f"DEBUG[{event_suffix}]: Downloading {endpoint} from URL: {media_url}")

        req = urllib.request.Request(media_url)
        req.add_header('User-Agent', 'FrigateNotifier/1.0')

        try:
            with urllib.request.urlopen(req, timeout=self.connection_timeout) as response:
                self.log(f"DEBUG[{event_suffix}]: {endpoint} HTTP response received (status: {response.status})")

                # Check for HTTP error status codes
                if response.status >= 400:
                    self.log(f"ERROR[{event_suffix}]: {endpoint} HTTP error {response.status} - treating as failed download")
                    raise Exception(f"HTTP Error {response.status}")

                content = response.read()
                file_size = len(content)

                # Check for empty or too small files based on file type
                min_size = 1000000 if endpoint == "clip.mp4" else 50000  # 1MB for video, 50KB for images
                if file_size < min_size:
                    self.log(f"ERROR[{event_suffix}]: {endpoint} file too small ({file_size} bytes) - treating as failed download (minimum: {min_size} bytes)")
                    raise ValueError(f"File too small: {file_size} bytes (minimum: {min_size} bytes)")

                with open(target_path, 'wb') as f:
                    f.write(content)

                self.log(f"DEBUG[{event_suffix}]: {endpoint} downloaded successfully ({file_size} bytes)")

            with self.cache_lock:
                self.file_cache[cache_key] = self._create_cache_entry(cache_key, target_path, file_size)

            relative_path = f"{camera}/{date_dir}/{filename}"
            self.log(f"DEBUG[{event_suffix}]: {endpoint} download complete, returning: {relative_path}")
            return relative_path

        except Exception as e:
            self.log(f"DEBUG[{event_suffix}]: {endpoint} download failed: {e}")
            # Clean up partial file if it exists
            if target_path.exists():
                target_path.unlink()
                self.log(f"DEBUG[{event_suffix}]: Removed partial {endpoint} file")
            raise



    def _send_notifications(self, event_data: Dict[str, Any], media_path: Optional[str], media_type: Optional[str], event_suffix: str) -> None:
        """Send notifications to configured persons."""
        if not self.person_configs:
            self.log(f"DEBUG[{event_suffix}]: No person configs found, skipping notifications")
            return

        self.log(f"DEBUG[{event_suffix}]: Starting notification sending process")
        self.log(f"DEBUG[{event_suffix}]: Media path: {media_path}, Media type: {media_type}")

        notification_start = time.time()
        timestamp = event_data["timestamp"].strftime("%H:%M:%S")
        camera = event_data["camera"]
        event_id = event_data["event_id"]
        label = event_data["label"]
        entered_zones = event_data["entered_zones"]
        zone_str = ", ".join(entered_zones) if entered_zones else "No zones"

        # Build notification data once
        camera_uri = f"homeassistant://navigate/dashboard-kameror/{camera}"
        channel = f"frigate-{camera}"

        self.log(f"DEBUG[{event_suffix}]: Built notification strings - Channel: {channel}")

        notification_data = {
            "actions": [{"action": "URI", "title": "Open Camera", "uri": camera_uri}],
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

        # Add media to notification
        if media_path and self.ext_domain:
            media_url = f"{self.ext_domain}/local/frigate/{media_path}"
            if media_type == "video":
                notification_data["video"] = media_url
                self.log(f"DEBUG[{event_suffix}]: Added video to notification: {media_url}")
            elif media_type == "image":
                notification_data["image"] = media_url
                self.log(f"DEBUG[{event_suffix}]: Added image to notification: {media_url}")
        else:
            self.log(f"DEBUG[{event_suffix}]: No media attached to notification (media_path: {media_path}, ext_domain: {self.ext_domain})")

        self.log(f"DEBUG[{event_suffix}]: Final notification data keys: {list(notification_data.keys())}")

        # Send to each configured person
        notifications_sent = 0
        for person_config in self.person_configs:
            if not person_config["enabled"]:
                self.log(f"DEBUG[{event_suffix}]: Skipping {person_config['name']} - disabled")
                continue

            if person_config["cameras"] and camera not in person_config["cameras"]:
                self.log(f"DEBUG[{event_suffix}]: Skipping {person_config['name']} - camera {camera} not in allowed cameras")
                continue

            if person_config["zones"] and not any(zone in person_config["zones"] for zone in entered_zones):
                self.log(f"DEBUG[{event_suffix}]: Skipping {person_config['name']} - no matching zones")
                continue

            if label not in person_config["labels"]:
                self.log(f"DEBUG[{event_suffix}]: Skipping {person_config['name']} - label {label} not in allowed labels")
                continue

            cooldown_key = f"{person_config['notify']}/{camera}"
            last_msg_time = time.time() - self.msg_cooldown.get(cooldown_key, 0)

            if last_msg_time < person_config["cooldown"]:
                self.log(f"DEBUG[{event_suffix}]: Skipping {person_config['name']} - cooldown active ({last_msg_time:.1f}s < {person_config['cooldown']}s)")
                continue

            title = f"{label} @ {camera}"
            message = f"{timestamp} - {zone_str} (ID: {event_id})"

            self.log(f"DEBUG[{event_suffix}]: Sending notification to {person_config['name']} via {person_config['notify']}")
            self.call_service(f"notify/{person_config['notify']}", title=title, message=message, data=notification_data)
            self.msg_cooldown[cooldown_key] = time.time()
            notifications_sent += 1

            # Log notification with media info
            media_info = f" - {media_type}: {media_path}" if media_path else " - no media"
            self.log(f"Notification sent to {person_config['name']} - {title} - Event ID: {event_id}{media_info}")

        self.log(f"DEBUG[{event_suffix}]: Sent {notifications_sent} notifications total")

        # Record notification time
        notification_time = time.time() - notification_start
        with self.metrics_lock:
            self.notification_times.append(notification_time)
            # Save metrics immediately after each notification
            self._save_todays_metrics()

        self.log(f"DEBUG[{event_suffix}]: Notification process completed in {notification_time:.3f}s")



    def _log_daily_metrics(self, **kwargs) -> None:
        """Log daily notification performance metrics and enrich previous day's data."""
        try:
            # First, enrich yesterday's data with calculated statistics
            self._enrich_yesterdays_metrics()

            with self.metrics_lock:
                if not self.notification_times and not self.delivery_times:
                    self.log("METRICS: No notifications sent today")
                    return

                # Calculate statistics
                stats = {}
                if self.notification_times:
                    stats.update({
                        "total_notifications": len(self.notification_times),
                        "min_seconds": round(min(self.notification_times), 3),
                        "avg_seconds": round(sum(self.notification_times) / len(self.notification_times), 3),
                        "max_seconds": round(max(self.notification_times), 3)
                    })

                if self.delivery_times:
                    stats.update({
                        "total_deliveries": len(self.delivery_times),
                        "min_delivery_seconds": round(min(self.delivery_times), 3),
                        "avg_delivery_seconds": round(sum(self.delivery_times) / len(self.delivery_times), 3),
                        "max_delivery_seconds": round(max(self.delivery_times), 3)
                    })

                # Prepare today's metrics
                today_metrics = {"date": datetime.now().strftime("%Y-%m-%d"), **stats}

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
                            "min_delivery_diff": round(stats["min_delivery_seconds"] - yesterday_metrics["min_delivery_seconds"], 3),
                            "avg_delivery_diff": round(stats["avg_delivery_seconds"] - yesterday_metrics["avg_delivery_seconds"], 3),
                            "max_delivery_diff": round(stats["max_delivery_seconds"] - yesterday_metrics["max_delivery_seconds"], 3)
                        })

                    if comparison:
                        today_metrics["comparison"] = comparison

                # Log metrics (data is already saved continuously)

                # Build log message efficiently
                log_parts = ["METRICS:"]

                if "total_notifications" in stats:
                    log_parts.append(f"Notifications={stats['total_notifications']}, Min={stats['min_seconds']}s, Avg={stats['avg_seconds']}s, Max={stats['max_seconds']}s")

                if "total_deliveries" in stats:
                    log_parts.append(f"Deliveries={stats['total_deliveries']}, Min={stats['min_delivery_seconds']}s, Avg={stats['avg_delivery_seconds']}s, Max={stats['max_delivery_seconds']}s")

                if comparison:
                    comp_parts = []
                    if "min_diff" in comparison:
                        comp_parts.append(f"Min diff: {comparison['min_diff']:+0.3f}s, Avg diff: {comparison['avg_diff']:+0.3f}s, Max diff: {comparison['max_diff']:+0.3f}s")
                    if "min_delivery_diff" in comparison:
                        comp_parts.append(f"Delivery diff: Min {comparison['min_delivery_diff']:+0.3f}s, Avg {comparison['avg_delivery_diff']:+0.3f}s, Max {comparison['max_delivery_diff']:+0.3f}s")
                    if comp_parts:
                        log_parts.append(" | ".join(comp_parts))

                self.log(" | ".join(log_parts))

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

                    self.log(f"DEBUG: Loaded {len(self.notification_times)} notification times and {len(self.delivery_times)} delivery times from today's metrics")
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
                    # Calculate statistics for notification_times
                    if "notification_times" in entry and entry["notification_times"]:
                        times = entry["notification_times"]
                        entry["notification_times"] = self._create_enriched_metrics(times, "notification")
                        self.log(f"Enriched yesterday's notification metrics: {len(times)} notifications, min={entry['notification_times']['min_notification_seconds']}s, avg={entry['notification_times']['avg_notification_seconds']}s, max={entry['notification_times']['max_notification_seconds']}s, std={entry['notification_times']['std_notification_seconds']}s")

                    # Calculate statistics for delivery_times
                    if "delivery_times" in entry and entry["delivery_times"]:
                        times = entry["delivery_times"]
                        entry["delivery_times"] = self._create_enriched_metrics(times, "delivery")
                        self.log(f"Enriched yesterday's delivery metrics: {len(times)} deliveries, min={entry['delivery_times']['min_delivery_seconds']}s, avg={entry['delivery_times']['avg_delivery_seconds']}s, max={entry['delivery_times']['max_delivery_seconds']}s, std={entry['delivery_times']['std_delivery_seconds']}s")

                    # Save the enriched data back to file
                    with open(self.metrics_file, 'w') as f:
                        json.dump(data, f, indent=2)

                    return

        except Exception as e:
            self.log(f"ERROR: Failed to enrich yesterday's metrics: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _calculate_std(self, values: list) -> float:
        """Calculate standard deviation of a list of values."""
        if len(values) < 2:
            return 0.0

        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5

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
            self.log(f"ERROR: Failed to load yesterday's metrics: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")
            return None



    def _cleanup_old_files(self, **kwargs) -> None:
        """Clean up old video files to prevent disk space issues."""
        if not self.snapshot_dir or not self.snapshot_dir.exists():
            return

        try:
            cutoff_time = datetime.now() - timedelta(days=self.max_file_age_days)
            files_removed = 0

            for file_path in self.snapshot_dir.rglob("*.mp4"):
                if file_path.stat().st_mtime < cutoff_time.timestamp():
                    file_path.unlink()
                    files_removed += 1

            if files_removed > 0:
                self.log(f"Cleaned up {files_removed} old video files")

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

    def _cleanup_old_tracked_events(self, **kwargs) -> None:
        """Clean up old tracked events to prevent memory leaks."""
        try:
            current_time = time.time()
            cutoff_time = current_time - 300  # Remove events older than 5 minutes

            with self.event_tracking_lock:
                events_to_remove = []
                for event_id, event_data in self.tracked_events.items():
                    if event_data["start_time"] < cutoff_time:
                        events_to_remove.append(event_id)

                for event_id in events_to_remove:
                    del self.tracked_events[event_id]

                if events_to_remove:
                    self.log(f"Cleaned up {len(events_to_remove)} old tracked events")

        except Exception as e:
            self.log(f"ERROR: Failed to cleanup tracked events: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def terminate(self) -> None:
        """Cleanup when app is terminated."""
        self.log("Shutting down Frigate Notifier...")

        self.shutdown_event.set()

        if self.processing_thread and self.processing_thread.is_alive():
            self.processing_thread.join(timeout=5)

        if hasattr(self, 'event_tracking_thread') and self.event_tracking_thread.is_alive():
            self.event_tracking_thread.join(timeout=5)

        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)

        self.log("Frigate Notifier terminated")

