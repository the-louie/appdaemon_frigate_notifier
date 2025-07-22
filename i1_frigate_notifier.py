"""
Frigate Notification App for AppDaemon

Copyright (c) 2025 the_louie
All rights reserved.

This app listens to Frigate MQTT events and sends notifications to configured users
when motion is detected. It supports zone filtering, cooldown periods, and video clip
downloads with enterprise-grade reliability and performance.

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
import json
import time
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from pathlib import Path
from dataclasses import dataclass
from collections import deque
import threading
from concurrent.futures import ThreadPoolExecutor
import hashlib
from enum import Enum
import urllib.request
import urllib.error


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


class ConnectionStatus(Enum):
    """Connection status enumeration."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"


@dataclass
class PersonConfig:
    """Configuration for a person to notify."""
    name: str
    notify: str
    labels: Set[str]
    cooldown: int
    zones: Optional[Set[str]] = None
    cameras: Optional[Set[str]] = None
    priority: EventPriority = EventPriority.NORMAL
    enabled: bool = True


@dataclass
class EventData:
    """Structured event data from Frigate."""
    event_id: str
    camera: str
    label: str
    entered_zones: List[str]
    event_type: str
    timestamp: datetime
    confidence: Optional[float] = None
    score: Optional[float] = None
    priority: EventPriority = EventPriority.NORMAL


@dataclass
class NotificationMetrics:
    """Metrics tracking for notifications."""
    total_events: int = 0
    events_processed: int = 0
    notifications_sent: int = 0
    downloads_successful: int = 0
    downloads_failed: int = 0
    cooldown_skipped: int = 0
    label_filtered: int = 0
    zone_filtered: int = 0
    errors: int = 0
    last_event_time: Optional[datetime] = None
    avg_processing_time: float = 0.0
    queue_size: int = 0
    connection_status: ConnectionStatus = ConnectionStatus.DISCONNECTED


@dataclass
class CacheEntry:
    """Cache entry for downloaded files."""
    file_path: str
    timestamp: datetime
    size: int
    checksum: str


class EventQueue:
    """Thread-safe event queue with priority handling."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.queue: deque = deque(maxlen=max_size)
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)

    def put(self, event: EventData) -> bool:
        """Add event to queue with priority ordering."""
        with self.lock:
            if len(self.queue) >= self.max_size:
                self.queue.popleft()

            # Optimized insertion: find position from end for better performance
            insert_pos = len(self.queue)
            for i in range(len(self.queue) - 1, -1, -1):
                if self.queue[i].priority.value >= event.priority.value:
                    insert_pos = i + 1
                    break
                insert_pos = i

            self.queue.insert(insert_pos, event)
            self.condition.notify()
            return True

    def get(self, timeout: Optional[float] = None) -> Optional[EventData]:
        """Get next event from queue."""
        with self.condition:
            if not self.queue:
                if timeout is None:
                    return None
                self.condition.wait(timeout)
                if not self.queue:
                    return None

            return self.queue.popleft()

    def size(self) -> int:
        """Get current queue size."""
        with self.lock:
            return len(self.queue)

    def clear(self) -> None:
        """Clear the queue."""
        with self.lock:
            self.queue.clear()


class FrigateNotification(hass.Hass):
    """AppDaemon app for sending Frigate motion notifications."""

    def initialize(self) -> None:
        """Initialize the app and set up MQTT listener."""
        self.metrics = NotificationMetrics()
        self.msg_cooldown: Dict[str, float] = {}
        self.person_configs: List[PersonConfig] = []
        self.connection_status = ConnectionStatus.DISCONNECTED
        self.event_queue = EventQueue(max_size=1000)
        self.processing_thread = None
        self.shutdown_event = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="FrigateNotifier")
        self.file_cache: Dict[str, CacheEntry] = {}
        self.cache_lock = threading.Lock()

        self._load_config()
        self._setup_mqtt()
        self._start_processing_thread()

        # Schedule periodic tasks
        self.run_every(self._cleanup_old_files, datetime.now(), 24 * 60 * 60)
        self.run_every(self._log_metrics, datetime.now(), 60 * 60)
        self.run_every(self._check_connection_health, datetime.now(), 5 * 60)
        self.run_every(self._cleanup_cache, datetime.now(), 6 * 60 * 60)

        self.log(f"Frigate Notifier initialized with {len(self.person_configs)} persons configured")

    def _load_config(self) -> None:
        """Load and validate configuration from args."""
        self.mqtt_topic = self.args.get("mqtt_topic", "frigate/events")
        self.frigate_url = self.args.get("frigate_url")
        self.ext_domain = self.args.get("ext_domain")
        self.snapshot_dir = self.args.get("snapshot_dir")

        # Validate required configuration
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
        self.enable_metrics = self.args.get("enable_metrics", True)
        self.cache_ttl_hours = self.args.get("cache_ttl_hours", 24)
        self.connection_timeout = self.args.get("connection_timeout", 30)

                # Priority configuration
        priority_config = self.args.get("priority", {})

        # Load zone priorities
        zone_priorities_raw = priority_config.get("zones", {})
        self.zone_priorities = {}
        for zone, priority_str in zone_priorities_raw.items():
            try:
                self.zone_priorities[zone] = EventPriority[priority_str.upper()]
            except (KeyError, AttributeError):
                self.zone_priorities[zone] = EventPriority.NORMAL

        # Load label priorities
        label_priorities_raw = priority_config.get("labels", {})
        self.label_priorities = {}
        for label, priority_str in label_priorities_raw.items():
            try:
                self.label_priorities[label] = EventPriority[priority_str.upper()]
            except (KeyError, AttributeError):
                self.label_priorities[label] = EventPriority.NORMAL

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
                priority_str = person_data.get("priority", "normal").upper()
                enabled = person_data.get("enabled", True)

                if not all([name, notify, labels]):
                    continue

                labels_set = set(labels)
                zones_set = set(person_data.get("zones", [])) if person_data.get("zones") else None
                cameras_set = set(person_data.get("cameras", [])) if person_data.get("cameras") else None

                try:
                    priority = EventPriority[priority_str]
                except KeyError:
                    priority = EventPriority.NORMAL

                person_config = PersonConfig(
                    name=name,
                    notify=notify,
                    labels=labels_set,
                    cooldown=cooldown,
                    zones=zones_set,
                    cameras=cameras_set,
                    priority=priority,
                    enabled=enabled
                )

                self.person_configs.append(person_config)

            except Exception as e:
                self.log(f"ERROR: Failed to load person config: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _setup_mqtt(self) -> None:
        """Set up MQTT connection and event listener."""
        try:
            self.connection_status = ConnectionStatus.CONNECTING
            self.mqtt = self.get_plugin_api("MQTT")

            if self.mqtt.is_client_connected():
                self.mqtt.mqtt_subscribe(f"{self.mqtt_topic}/#")
                self.mqtt.listen_event(self._handle_mqtt_message, "MQTT_MESSAGE")
                self.connection_status = ConnectionStatus.CONNECTED
            else:
                self.connection_status = ConnectionStatus.ERROR

        except Exception as e:
            self.connection_status = ConnectionStatus.ERROR
            self.log(f"ERROR: Failed to set up MQTT: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _start_processing_thread(self) -> None:
        """Start the event processing thread."""
        self.processing_thread = threading.Thread(
            target=self._event_processing_worker,
            name="EventProcessor",
            daemon=True
        )
        self.processing_thread.start()

    def _event_processing_worker(self) -> None:
        """Worker thread for processing events from the queue."""
        while not self.shutdown_event.is_set():
            try:
                event = self.event_queue.get(timeout=1.0)
                if event:
                    self._process_event(event)
            except Exception as e:
                self.log(f"ERROR: Event processing worker error: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")
                self.metrics.errors += 1

    def _handle_mqtt_message(self, event_name: str, data: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        """Handle incoming MQTT messages from Frigate."""
        try:
            self.metrics.total_events += 1
            self.metrics.last_event_time = datetime.now()

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
                    self.metrics.errors += 1
                    return

            event_data = self._extract_event_data(payload)
            if not event_data:
                return

            if not self.event_queue.put(event_data):
                self.log("WARNING: Event queue full, dropping event", level="WARNING")
            else:
                self.metrics.queue_size = self.event_queue.size()

        except Exception as e:
            self.log(f"ERROR: Failed to process MQTT message: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")
            self.metrics.errors += 1

    def _extract_event_data(self, payload: Dict[str, Any]) -> Optional[EventData]:
        """Extract and validate event data from payload."""
        try:
            event_data = payload.get("after", {})
            event_type = payload.get("type", "")
            event_id = event_data.get("id")
            camera = event_data.get("camera", "Unknown")
            label = event_data.get("label", "Unknown")
            entered_zones = event_data.get("entered_zones", [])
            confidence = event_data.get("confidence")
            score = event_data.get("score")

            if not event_id:
                return None

            # Determine priority based on zones and labels
            zone_priority = EventPriority.NORMAL
            for zone in entered_zones:
                if zone in self.zone_priorities:
                    if self.zone_priorities[zone].value > zone_priority.value:
                        zone_priority = self.zone_priorities[zone]

            label_priority = self.label_priorities.get(label, EventPriority.NORMAL)

            # Use the highest priority between zone and label
            if label_priority.value > zone_priority.value:
                priority = label_priority
            else:
                priority = zone_priority

            return EventData(
                event_id=event_id,
                camera=camera,
                label=label,
                entered_zones=entered_zones,
                event_type=event_type,
                timestamp=datetime.now(),
                confidence=confidence,
                score=score,
                priority=priority
            )

        except Exception as e:
            self.log(f"ERROR: Failed to extract event data: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")
            return None

    def _process_event(self, event_data: EventData) -> None:
        """Process a Frigate event."""
        start_time = time.time()

        try:
            self.metrics.events_processed += 1

            if not event_data.event_id or event_data.event_type != "end":
                return

            if self.only_zones and not event_data.entered_zones:
                self.metrics.zone_filtered += 1
                return

            # Download media and send notifications
            self.executor.submit(self._download_and_notify, event_data)

            processing_time = time.time() - start_time
            self.metrics.avg_processing_time = (
                (self.metrics.avg_processing_time * (self.metrics.events_processed - 1) + processing_time)
                / self.metrics.events_processed
            )

        except Exception as e:
            self.log(f"ERROR: Failed to process event: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")
            self.metrics.errors += 1

    def _download_and_notify(self, event_data: EventData) -> None:
        """Download media and send notifications."""
        try:
            # Try to download video first with 30s timeout and 2s retries
            video_path = self._download_media_with_retry(event_data.event_id, event_data.camera, "clip.mp4", ".mp4")

            if video_path:
                self._send_notifications(event_data, video_path, "video")
                return

            # Fallback to snapshot if video download failed
            snapshot_path = self._download_media_with_retry(event_data.event_id, event_data.camera, "snapshot.jpg", ".jpg")
            if snapshot_path:
                self._send_notifications(event_data, snapshot_path, "image")
            else:
                # Send notification without media if both downloads failed
                self._send_notifications(event_data, None, None)

        except Exception as e:
            self.log(f"ERROR: Failed to download and notify for event {event_data.event_id}: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")
            self.metrics.errors += 1

    def _download_media_with_retry(self, event_id: str, camera: str, endpoint: str, extension: str) -> Optional[str]:
        """Download media with 30s timeout and 2s retries."""
        start_time = time.time()
        timeout = 30
        retry_interval = 2

        while time.time() - start_time < timeout:
            try:
                media_path = self._download_media(event_id, camera, endpoint, extension)
                if media_path:
                    return media_path
            except Exception as e:
                self.log(f"ERROR: Media download attempt failed for event {event_id}: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")
                self.metrics.downloads_failed += 1

            time.sleep(retry_interval)

        self.log(f"Media download timeout after {timeout}s for event {event_id}")
        return None

    def _download_media(self, event_id: str, camera: str, endpoint: str, extension: str) -> Optional[str]:
        """Download media file from Frigate."""
        cache_key = f"{event_id}_{camera}_{endpoint}"

        # Check cache first
        with self.cache_lock:
            if cache_key in self.file_cache:
                cache_entry = self.file_cache[cache_key]
                if (datetime.now() - cache_entry.timestamp).total_seconds() < self.cache_ttl_hours * 3600:
                    self.metrics.downloads_successful += 1
                    return cache_entry.file_path

        # Download new media
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H:%M:%S")
        date_dir = now.strftime("%Y-%m-%d")
        target_dir = self.snapshot_dir / camera / date_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{timestamp}--{event_id}{extension}"
        target_path = target_dir / filename

        if target_path.exists():
            self._add_to_cache(cache_key, str(target_path), target_path.stat().st_size)
            self.metrics.downloads_successful += 1
            return f"{camera}/{date_dir}/{filename}"

        media_url = f"{self.frigate_url}/{event_id}/{endpoint}"
        req = urllib.request.Request(media_url)
        req.add_header('User-Agent', 'FrigateNotifier/1.0')

        with urllib.request.urlopen(req, timeout=self.connection_timeout) as response:
            with open(target_path, 'wb') as f:
                f.write(response.read())

        file_size = target_path.stat().st_size
        self._add_to_cache(cache_key, str(target_path), file_size)
        self.metrics.downloads_successful += 1

        return f"{camera}/{date_dir}/{filename}"

    def _add_to_cache(self, cache_key: str, file_path: str, file_size: int) -> None:
        """Add file to cache."""
        with self.cache_lock:
            checksum = hashlib.md5(f"{cache_key}_{file_size}".encode()).hexdigest()
            self.file_cache[cache_key] = CacheEntry(
                file_path=file_path,
                timestamp=datetime.now(),
                size=file_size,
                checksum=checksum
            )

    def _send_notifications(self, event_data: EventData, media_path: Optional[str], media_type: Optional[str]) -> None:
        """Send notifications to configured persons."""
        timestamp = event_data.timestamp.strftime("%H:%M:%S")
        zone_str = ", ".join(event_data.entered_zones) if event_data.entered_zones else "No zones"

        # Build notification data
        notification_data = {
            "actions": [{
                "action": "URI",
                "title": "Open Camera",
                "uri": f"homeassistant://navigate/dashboard-kameror/{event_data.camera}"
            }],
            "channel": f"frigate-{event_data.camera}",
            "importance": "high",
            "visibility": "public",
            "priority": "high",
            "ttl": 0,
            "notification_icon": self.cam_icons.get(event_data.camera, "mdi:cctv")
        }

        # Add media to notification
        if media_path and self.ext_domain:
            if media_type == "video":
                notification_data["video"] = f"{self.ext_domain}/local/frigate/{media_path}"
            elif media_type == "image":
                notification_data["image"] = f"{self.ext_domain}/local/frigate/{media_path}"

        # Send to each configured person
        for person_config in self.person_configs:
            if not person_config.enabled:
                continue

            if person_config.cameras and event_data.camera not in person_config.cameras:
                continue

            if person_config.zones and not any(zone in person_config.zones for zone in event_data.entered_zones):
                continue

            if event_data.label not in person_config.labels:
                self.metrics.label_filtered += 1
                continue

            cooldown_key = f"{person_config.notify}/{event_data.camera}"
            last_msg_time = time.time() - self.msg_cooldown.get(cooldown_key, 0)

            if last_msg_time < person_config.cooldown:
                self.metrics.cooldown_skipped += 1
                continue

            title = f"{event_data.label} @ {event_data.camera}"
            message = f"{timestamp} - {zone_str} (ID: {event_data.event_id})"

            self.call_service(
                f"notify/{person_config.notify}",
                title=title,
                message=message,
                data=notification_data
            )

            self.msg_cooldown[cooldown_key] = time.time()
            self.metrics.notifications_sent += 1

            # Log notification with media info
            media_info = f" - {media_type}: {media_path}" if media_path else " - no media"
            self.log(f"Notification sent to {person_config.name} - {title} - Event ID: {event_data.event_id}{media_info}")

    def _check_connection_health(self, kwargs: Dict[str, Any]) -> None:
        """Check and report connection health."""
        try:
            if hasattr(self, 'mqtt') and self.mqtt.is_client_connected():
                if self.connection_status != ConnectionStatus.CONNECTED:
                    self.connection_status = ConnectionStatus.CONNECTED
            else:
                if self.connection_status == ConnectionStatus.CONNECTED:
                    self.connection_status = ConnectionStatus.ERROR

        except Exception as e:
            self.log(f"ERROR: Connection health check failed: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

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
            expired_keys = []
            max_cache_size = 1000

            with self.cache_lock:
                # Remove expired entries
                for key, entry in self.file_cache.items():
                    if entry.timestamp < cutoff_time:
                        expired_keys.append(key)

                for key in expired_keys:
                    del self.file_cache[key]

                # Limit cache size if needed
                if len(self.file_cache) > max_cache_size:
                    sorted_entries = sorted(self.file_cache.items(), key=lambda x: x[1].timestamp)
                    entries_to_remove = len(self.file_cache) - max_cache_size
                    for i in range(entries_to_remove):
                        del self.file_cache[sorted_entries[i][0]]

            if expired_keys:
                self.log(f"Cleaned up {len(expired_keys)} expired cache entries")

        except Exception as e:
            self.log(f"ERROR: Failed to cleanup cache: {e} (line {sys.exc_info()[2].tb_lineno})", level="ERROR")

    def _log_metrics(self, kwargs: Dict[str, Any]) -> None:
        """Log metrics for monitoring."""
        if not self.enable_metrics:
            return

        self.log(f"METRICS: Events={self.metrics.total_events}, "
                f"Processed={self.metrics.events_processed}, "
                f"Notifications={self.metrics.notifications_sent}, "
                f"Downloads={self.metrics.downloads_successful}/{self.metrics.downloads_failed}, "
                f"CooldownSkipped={self.metrics.cooldown_skipped}, "
                f"LabelFiltered={self.metrics.label_filtered}, "
                f"ZoneFiltered={self.metrics.zone_filtered}, "
                f"Errors={self.metrics.errors}, "
                f"QueueSize={self.event_queue.size()}, "
                f"AvgProcessingTime={self.metrics.avg_processing_time:.3f}s, "
                f"Connection={self.connection_status.value}")

    def terminate(self) -> None:
        """Cleanup when app is terminated."""
        self.log("Shutting down Frigate Notifier...")

        self.shutdown_event.set()

        if self.processing_thread and self.processing_thread.is_alive():
            self.processing_thread.join(timeout=5)

        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)

        self.log("Frigate Notifier terminated")

