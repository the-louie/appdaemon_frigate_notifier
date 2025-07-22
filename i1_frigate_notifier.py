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
        self.metrics_lock = threading.Lock()
        self.metrics_file = Path(__file__).parent / "notification_metrics.json"

        self._load_config()
        self._setup_mqtt()

        # Start processing thread
        self.processing_thread = threading.Thread(target=self._event_processing_worker, name="EventProcessor", daemon=True)
        self.processing_thread.start()

        # Schedule periodic tasks
        self.run_every(self._cleanup_old_files, datetime.now(), 24 * 60 * 60)
        self.run_every(self._log_daily_metrics, datetime.now().replace(hour=23, minute=59, second=0, microsecond=0), 24 * 60 * 60)
        self.run_every(self._cleanup_cache, datetime.now(), 6 * 60 * 60)

        self.log(f"Frigate Notifier initialized with {len(self.person_configs)} persons configured")

    def _load_config(self) -> None:
        """Load and validate configuration from args."""
        self.mqtt_topic = self.args.get("mqtt_topic", "frigate/events")
        self.frigate_url = self.args.get("frigate_url")
        self.ext_domain = self.args.get("ext_domain")
        self.snapshot_dir = self.args.get("snapshot_dir")

        if not self.frigate_url:
            self.log("ERROR: frigate_url is required", level="ERROR")
            return

        if not self.ext_domain:
            self.log("ERROR: ext_domain is required", level="ERROR")
            return

        if self.snapshot_dir:
            self.snapshot_dir = Path(self.snapshot_dir)
            if not self.snapshot_dir.exists():
                self.snapshot_dir.mkdir(parents=True, exist_ok=True)

        self.only_zones = self.args.get("only_zones", False)
        self.cam_icons = self.args.get("cam_icons", {})
        self._load_person_configs()

        self.max_file_age_days = self.args.get("max_file_age_days", 30)
        self.cache_ttl_hours = self.args.get("cache_ttl_hours", 24)
        self.connection_timeout = self.args.get("connection_timeout", 30)

        # Load priority configuration
        priority_config = self.args.get("priority", {})
        self.zone_priorities = {zone: priority_str.upper() for zone, priority_str in priority_config.get("zones", {}).items()}
        self.label_priorities = {label: priority_str.upper() for label, priority_str in priority_config.get("labels", {}).items()}

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

    def _handle_mqtt_message(self, event_name: str, data: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        """Handle incoming MQTT messages from Frigate."""
        try:
            if not data or 'topic' not in data or 'payload' not in data:
                return

            topic = data['topic']
            payload = data['payload']

            if not topic.startswith(self.mqtt_topic):
                return

            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    self.log("ERROR: Invalid JSON payload", level="ERROR")
                    return

            event_data = self._extract_event_data(payload)
            if not event_data:
                return

            with self.queue_lock:
                if len(self.event_queue) < 1000:
                    self.event_queue.append(event_data)

        except Exception as e:
            self.log(f"ERROR: Failed to process MQTT message: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

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
        try:
            if not event_data["event_id"] or event_data["event_type"] != "end":
                return

            if self.only_zones and not event_data["entered_zones"]:
                return

            # Download media and send notifications
            self.executor.submit(self._download_and_notify, event_data)

        except Exception as e:
            self.log(f"ERROR: Failed to process event: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _download_and_notify(self, event_data: Dict[str, Any]) -> None:
        """Download media and send notifications."""
        try:
            # Try video first, then snapshot, then no media
            for endpoint, extension, media_type in [("clip.mp4", ".mp4", "video"), ("snapshot.jpg", ".jpg", "image")]:
                media_path = self._download_media_with_retry(event_data["event_id"], event_data["camera"], endpoint, extension)
                if media_path:
                    self._send_notifications(event_data, media_path, media_type)
                    return

            # Send notification without media if both downloads failed
            self._send_notifications(event_data, None, None)

        except Exception as e:
            self.log(f"ERROR: Failed to download and notify for event {event_data['event_id']}: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _download_media_with_retry(self, event_id: str, camera: str, endpoint: str, extension: str) -> Optional[str]:
        """Download media with 30s timeout and 2s retries."""
        start_time = time.time()
        while time.time() - start_time < 30:
            try:
                media_path = self._download_media(event_id, camera, endpoint, extension)
                if media_path:
                    return media_path
            except Exception as e:
                self.log(f"ERROR: Media download attempt failed for event {event_id}: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")
            time.sleep(2)

        self.log(f"Media download timeout after 30s for event {event_id}")
        return None

    def _download_media(self, event_id: str, camera: str, endpoint: str, extension: str) -> Optional[str]:
        """Download media file from Frigate."""
        cache_key = f"{event_id}_{camera}_{endpoint}"

        # Check cache first
        with self.cache_lock:
            if cache_key in self.file_cache:
                cache_entry = self.file_cache[cache_key]
                if (datetime.now() - cache_entry["timestamp"]).total_seconds() < self.cache_ttl_hours * 3600:
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
            # Add to cache inline
            with self.cache_lock:
                self.file_cache[cache_key] = {
                    "file_path": str(target_path),
                    "timestamp": datetime.now(),
                    "size": target_path.stat().st_size,
                    "checksum": hashlib.md5(f"{cache_key}_{target_path.stat().st_size}".encode()).hexdigest()
                }
            return f"{camera}/{date_dir}/{filename}"

        media_url = f"{self.frigate_url}/{event_id}/{endpoint}"
        req = urllib.request.Request(media_url)
        req.add_header('User-Agent', 'FrigateNotifier/1.0')

        with urllib.request.urlopen(req, timeout=self.connection_timeout) as response:
            with open(target_path, 'wb') as f:
                f.write(response.read())

        # Add to cache inline
        with self.cache_lock:
            self.file_cache[cache_key] = {
                "file_path": str(target_path),
                "timestamp": datetime.now(),
                "size": target_path.stat().st_size,
                "checksum": hashlib.md5(f"{cache_key}_{target_path.stat().st_size}".encode()).hexdigest()
            }
        return f"{camera}/{date_dir}/{filename}"

    def _send_notifications(self, event_data: Dict[str, Any], media_path: Optional[str], media_type: Optional[str]) -> None:
        """Send notifications to configured persons."""
        notification_start = time.time()
        timestamp = event_data["timestamp"].strftime("%H:%M:%S")
        zone_str = ", ".join(event_data["entered_zones"]) if event_data["entered_zones"] else "No zones"

        # Build notification data
        notification_data = {
            "actions": [{"action": "URI", "title": "Open Camera", "uri": f"homeassistant://navigate/dashboard-kameror/{event_data['camera']}"}],
            "channel": f"frigate-{event_data['camera']}",
            "importance": "high",
            "visibility": "public",
            "priority": "high",
            "ttl": 0,
            "notification_icon": self.cam_icons.get(event_data["camera"], "mdi:cctv")
        }

        # Add media to notification
        if media_path and self.ext_domain:
            if media_type == "video":
                notification_data["video"] = f"{self.ext_domain}/local/frigate/{media_path}"
            elif media_type == "image":
                notification_data["image"] = f"{self.ext_domain}/local/frigate/{media_path}"

        # Send to each configured person
        for person_config in self.person_configs:
            if not person_config["enabled"]:
                continue

            if person_config["cameras"] and event_data["camera"] not in person_config["cameras"]:
                continue

            if person_config["zones"] and not any(zone in person_config["zones"] for zone in event_data["entered_zones"]):
                continue

            if event_data["label"] not in person_config["labels"]:
                continue

            cooldown_key = f"{person_config['notify']}/{event_data['camera']}"
            last_msg_time = time.time() - self.msg_cooldown.get(cooldown_key, 0)

            if last_msg_time < person_config["cooldown"]:
                continue

            title = f"{event_data['label']} @ {event_data['camera']}"
            message = f"{timestamp} - {zone_str} (ID: {event_data['event_id']})"

            self.call_service(f"notify/{person_config['notify']}", title=title, message=message, data=notification_data)
            self.msg_cooldown[cooldown_key] = time.time()

            # Log notification with media info
            media_info = f" - {media_type}: {media_path}" if media_path else " - no media"
            self.log(f"Notification sent to {person_config['name']} - {title} - Event ID: {event_data['event_id']}{media_info}")

        # Record notification time
        notification_time = time.time() - notification_start
        with self.metrics_lock:
            self.notification_times.append(notification_time)

    def _log_daily_metrics(self, kwargs: Dict[str, Any]) -> None:
        """Log daily notification performance metrics."""
        try:
            with self.metrics_lock:
                if not self.notification_times:
                    self.log("METRICS: No notifications sent today")
                    return

                # Calculate statistics
                min_time = min(self.notification_times)
                max_time = max(self.notification_times)
                avg_time = sum(self.notification_times) / len(self.notification_times)

                # Load previous day's metrics for comparison
                yesterday_metrics = self._load_yesterday_metrics()

                # Prepare today's metrics
                today_metrics = {
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "total_notifications": len(self.notification_times),
                    "min_seconds": round(min_time, 3),
                    "avg_seconds": round(avg_time, 3),
                    "max_seconds": round(max_time, 3)
                }

                # Add comparison if yesterday's data exists
                if yesterday_metrics:
                    today_metrics["comparison"] = {
                        "min_diff": round(min_time - yesterday_metrics["min_seconds"], 3),
                        "avg_diff": round(avg_time - yesterday_metrics["avg_seconds"], 3),
                        "max_diff": round(max_time - yesterday_metrics["max_seconds"], 3)
                    }

                # Save to file
                self._save_metrics(today_metrics)

                # Log metrics
                log_msg = f"METRICS: Notifications={today_metrics['total_notifications']}, Min={today_metrics['min_seconds']}s, Avg={today_metrics['avg_seconds']}s, Max={today_metrics['max_seconds']}s"
                if yesterday_metrics:
                    log_msg += f" | Min diff: {today_metrics['comparison']['min_diff']:+0.3f}s, Avg diff: {today_metrics['comparison']['avg_diff']:+0.3f}s, Max diff: {today_metrics['comparison']['max_diff']:+0.3f}s"

                self.log(log_msg)

                # Clear today's data for next day
                self.notification_times.clear()

        except Exception as e:
            self.log(f"ERROR: Failed to log daily metrics: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

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
                    return entry

            return None

        except Exception as e:
            self.log(f"ERROR: Failed to load yesterday's metrics: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")
            return None

    def _save_metrics(self, today_metrics: Dict[str, Any]) -> None:
        """Save metrics to JSON file."""
        try:
            # Load existing data
            if self.metrics_file.exists():
                with open(self.metrics_file, 'r') as f:
                    data = json.load(f)
            else:
                data = {"daily_metrics": []}

            # Add today's metrics
            data["daily_metrics"].append(today_metrics)

            # Keep only last 30 days
            if len(data["daily_metrics"]) > 30:
                data["daily_metrics"] = data["daily_metrics"][-30:]

            # Save to file
            with open(self.metrics_file, 'w') as f:
                json.dump(data, f, indent=2)

        except Exception as e:
            self.log(f"ERROR: Failed to save metrics: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _cleanup_old_files(self, kwargs: Dict[str, Any]) -> None:
        """Clean up old video files to prevent disk space issues."""
        if not self.snapshot_dir or not self.snapshot_dir.exists():
            return

        try:
            cutoff_date = datetime.now() - timedelta(days=self.max_file_age_days)
            files_removed = 0

            for file_path in self.snapshot_dir.rglob("*.mp4"):
                if file_path.stat().st_mtime < cutoff_date.timestamp():
                    file_path.unlink()
                    files_removed += 1

            if files_removed > 0:
                self.log(f"Cleaned up {files_removed} old video files")

        except Exception as e:
            self.log(f"ERROR: Failed to cleanup old files: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _cleanup_cache(self, kwargs: Dict[str, Any]) -> None:
        """Clean up expired cache entries and limit cache size."""
        try:
            cutoff_time = datetime.now() - timedelta(hours=self.cache_ttl_hours)
            expired_keys = [key for key, entry in self.file_cache.items() if entry["timestamp"] < cutoff_time]

            with self.cache_lock:
                for key in expired_keys:
                    del self.file_cache[key]

                # Limit cache size if needed
                if len(self.file_cache) > 1000:
                    sorted_entries = sorted(self.file_cache.items(), key=lambda x: x[1]["timestamp"])
                    entries_to_remove = len(self.file_cache) - 1000
                    for i in range(entries_to_remove):
                        del self.file_cache[sorted_entries[i][0]]

            if expired_keys:
                self.log(f"Cleaned up {len(expired_keys)} expired cache entries")

        except Exception as e:
            self.log(f"ERROR: Failed to cleanup cache: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def terminate(self) -> None:
        """Cleanup when app is terminated."""
        self.log("Shutting down Frigate Notifier...")

        self.shutdown_event.set()

        if self.processing_thread and self.processing_thread.is_alive():
            self.processing_thread.join(timeout=5)

        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)

        self.log("Frigate Notifier terminated")

