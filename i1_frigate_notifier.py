"""
Frigate Notification App for AppDaemon

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
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
import requests
from pathlib import Path
from dataclasses import dataclass
from collections import deque
import threading
from concurrent.futures import ThreadPoolExecutor
import hashlib
from enum import Enum


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
                # Remove lowest priority event if queue is full
                self.queue.popleft()

            # Insert based on priority (higher priority first)
            insert_pos = 0
            for i, existing_event in enumerate(self.queue):
                if event.priority.value > existing_event.priority.value:
                    insert_pos = i
                    break
                insert_pos = i + 1

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
        # Initialize metrics
        self.metrics = NotificationMetrics()

        # Initialize state
        self.msg_cooldown: Dict[str, float] = {}
        self.person_configs: List[PersonConfig] = []
        self.connection_status = ConnectionStatus.DISCONNECTED

        # Event queue for processing
        self.event_queue = EventQueue(max_size=1000)
        self.processing_thread = None
        self.shutdown_event = threading.Event()

        # Thread pool for async operations
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="FrigateNotifier")

        # File cache for avoiding duplicate downloads
        self.file_cache: Dict[str, CacheEntry] = {}
        self.cache_lock = threading.Lock()

        # Load configuration
        self._load_config()

        # Set up MQTT connection
        self._setup_mqtt()

        # Start event processing thread
        self._start_processing_thread()

        # Schedule periodic tasks
        self.run_every(self._cleanup_old_files, datetime.now(), 24 * 60 * 60)  # Daily
        self.run_every(self._log_metrics, datetime.now(), 60 * 60)  # Hourly
        self.run_every(self._check_connection_health, datetime.now(), 5 * 60)  # Every 5 minutes
        self.run_every(self._cleanup_cache, datetime.now(), 6 * 60 * 60)  # Every 6 hours

        # Log initialization
        self.log(f"Frigate Notifier initialized with {len(self.person_configs)} persons configured")
        self.log(f"MQTT Topic: {self.mqtt_topic}")
        self.log(f"Only zones: {self.only_zones}")
        self.log("Max download workers: 5")
        self.log("Event queue size: 1000")

    def _load_config(self) -> None:
        """Load and validate configuration from args."""
        # MQTT Configuration
        self.mqtt_topic = self.args.get("mqtt_topic", "frigate/events")

        # Frigate Configuration
        self.frigate_url = self.args.get("frigate_url")
        if not self.frigate_url:
            self.log("WARNING: frigate_url not configured - video downloads disabled", level="WARNING")

        # External domain for notification links
        self.ext_domain = self.args.get("ext_domain")
        if not self.ext_domain:
            self.log("WARNING: ext_domain not configured - notification links may not work", level="WARNING")

        # Snapshot directory
        self.snapshot_dir = self.args.get("snapshot_dir")
        if self.snapshot_dir:
            self.snapshot_dir = Path(self.snapshot_dir)
            if not self.snapshot_dir.exists():
                self.log(f"Creating snapshot directory: {self.snapshot_dir}")
                self.snapshot_dir.mkdir(parents=True, exist_ok=True)

        # Zone filtering
        self.only_zones = self.args.get("only_zones", False)

        # Camera icons
        self.cam_icons = self.args.get("cam_icons", {})

        # Load and validate person configurations
        self._load_person_configs()

        # Load additional settings
        self.max_retries = self.args.get("max_retries", 3)
        self.retry_delay = self.args.get("retry_delay", 5)
        self.max_file_age_days = self.args.get("max_file_age_days", 30)
        self.enable_metrics = self.args.get("enable_metrics", True)
        self.cache_ttl_hours = self.args.get("cache_ttl_hours", 24)
        self.connection_timeout = self.args.get("connection_timeout", 30)

    def _load_person_configs(self) -> None:
        """Load and validate person configurations."""
        persons_raw = self.args.get("persons", [])
        if not persons_raw:
            self.log("WARNING: No persons configured - no notifications will be sent", level="WARNING")
            return

        for person_data in persons_raw:
            try:
                # Validate required fields
                name = person_data.get("name")
                notify = person_data.get("notify")
                labels = person_data.get("labels", [])
                cooldown = person_data.get("cooldown", 0)
                priority_str = person_data.get("priority", "normal").upper()
                enabled = person_data.get("enabled", True)

                if not name:
                    self.log("WARNING: Person missing name field", level="WARNING")
                    continue

                if not notify:
                    self.log(f"WARNING: Person {name} missing notify address", level="WARNING")
                    continue

                if not labels:
                    self.log(f"WARNING: Person {name} missing labels filter", level="WARNING")
                    continue

                # Convert to sets for efficient lookups
                labels_set = set(labels)
                zones_set = set(person_data.get("zones", [])) if person_data.get("zones") else None
                cameras_set = set(person_data.get("cameras", [])) if person_data.get("cameras") else None

                # Parse priority
                try:
                    priority = EventPriority[priority_str]
                except KeyError:
                    self.log(f"WARNING: Invalid priority '{priority_str}' for {name}, using NORMAL", level="WARNING")
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
                self.log(f"Loaded person config: {name} -> {notify} (labels: {labels}, cooldown: {cooldown}s, priority: {priority.name})")

            except Exception as e:
                self.log(f"ERROR: Failed to load person config: {e}", level="ERROR")

    def _setup_mqtt(self) -> None:
        """Set up MQTT connection and event listener."""
        try:
            self.connection_status = ConnectionStatus.CONNECTING
            self.mqtt = self.get_plugin_api("MQTT")

            if self.mqtt.is_client_connected():
                self.mqtt.mqtt_subscribe(f"{self.mqtt_topic}/#")
                self.mqtt.listen_event(self._handle_mqtt_message, "MQTT_MESSAGE")
                self.connection_status = ConnectionStatus.CONNECTED
                self.log("MQTT connection established successfully")
            else:
                self.connection_status = ConnectionStatus.ERROR
                self.log("ERROR: MQTT client not connected", level="ERROR")

        except Exception as e:
            self.connection_status = ConnectionStatus.ERROR
            self.log(f"ERROR: Failed to set up MQTT: {e}", level="ERROR")

    def _start_processing_thread(self) -> None:
        """Start the event processing thread."""
        self.processing_thread = threading.Thread(
            target=self._event_processing_worker,
            name="EventProcessor",
            daemon=True
        )
        self.processing_thread.start()
        self.log("Event processing thread started")

    def _event_processing_worker(self) -> None:
        """Worker thread for processing events from the queue."""
        while not self.shutdown_event.is_set():
            try:
                event = self.event_queue.get(timeout=1.0)
                if event:
                    self._process_event(event)
            except Exception as e:
                self.log(f"ERROR: Event processing worker error: {e}", level="ERROR")
                self.metrics.errors += 1

    def _handle_mqtt_message(self, event_name: str, data: Dict[str, Any], kwargs: Dict[str, Any]) -> None:
        """Handle incoming MQTT messages from Frigate."""
        try:
            self.metrics.total_events += 1
            self.metrics.last_event_time = datetime.now()

            # Validate message structure
            if not data or 'topic' not in data or 'payload' not in data:
                return

            topic = data['topic']
            payload = data['payload']

            # Check if this is a Frigate event
            if not topic.startswith(self.mqtt_topic):
                return

            # Parse payload
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    self.log(f"ERROR: Invalid JSON payload: {payload}", level="ERROR")
                    self.metrics.errors += 1
                    return

            # Extract and validate event data
            event_data = self._extract_event_data(payload)
            if not event_data:
                return

            # Log event details
            self.log(f"Event: {event_data.event_id} | Camera: {event_data.camera} | "
                    f"Label: {event_data.label} | Zones: {event_data.entered_zones} | "
                    f"Type: {event_data.event_type} | Priority: {event_data.priority.name}")

            # Add to processing queue
            if not self.event_queue.put(event_data):
                self.log("WARNING: Event queue full, dropping event", level="WARNING")
            else:
                self.metrics.queue_size = self.event_queue.size()

        except Exception as e:
            self.log(f"ERROR: Failed to process MQTT message: {e}", level="ERROR")
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
                self.log("ERROR: Event ID is missing", level="ERROR")
                return None

            # Determine priority based on label and zones
            priority = self._determine_event_priority(label, entered_zones)

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
            self.log(f"ERROR: Failed to extract event data: {e}", level="ERROR")
            return None

    def _determine_event_priority(self, label: str, zones: List[str]) -> EventPriority:
        """Determine event priority based on label and zones."""
        # Critical zones (front door, driveway)
        critical_zones = {"front_door", "driveway", "entrance"}
        if any(zone in critical_zones for zone in zones):
            return EventPriority.CRITICAL

        # High priority labels
        high_priority_labels = {"person", "car", "truck"}
        if label in high_priority_labels:
            return EventPriority.HIGH

        # Normal priority for other events
        return EventPriority.NORMAL

    def _process_event(self, event_data: EventData) -> None:
        """Process a Frigate event."""
        start_time = time.time()

        try:
            self.metrics.events_processed += 1

            # Validate event
            if not event_data.event_id:
                self.log("ERROR: Event ID is missing", level="ERROR")
                return

            # Only process 'end' events
            if event_data.event_type != "end":
                self.log(f"Skipping {event_data.event_type} event (only processing 'end' events)")
                return

            # Check zone filtering
            if self.only_zones and not event_data.entered_zones:
                self.log("Skipping event - no zones entered and only_zones=True")
                self.metrics.zone_filtered += 1
                return

            # Download video clip if configured (async)
            event_url = None
            if self.frigate_url and self.snapshot_dir:
                # Submit download to thread pool
                future = self.executor.submit(self._download_video_clip, event_data.event_id, event_data.camera)
                event_url = future.result(timeout=60)  # Wait up to 60 seconds

            # Send notifications
            self._send_notifications(event_data, event_url)

            # Update processing time metrics
            processing_time = time.time() - start_time
            self.metrics.avg_processing_time = (
                (self.metrics.avg_processing_time * (self.metrics.events_processed - 1) + processing_time)
                / self.metrics.events_processed
            )

        except Exception as e:
            self.log(f"ERROR: Failed to process event: {e}", level="ERROR")
            self.metrics.errors += 1

    def _download_video_clip(self, event_id: str, camera: str) -> Optional[str]:
        """Download video clip from Frigate with retry logic and caching."""
        # Check cache first
        cache_key = f"{event_id}_{camera}"
        with self.cache_lock:
            if cache_key in self.file_cache:
                cache_entry = self.file_cache[cache_key]
                if (datetime.now() - cache_entry.timestamp).total_seconds() < self.cache_ttl_hours * 3600:
                    self.log(f"Using cached video clip: {cache_entry.file_path}")
                    self.metrics.downloads_successful += 1
                    return cache_entry.file_path

        for attempt in range(self.max_retries):
            try:
                # Create timestamp for filename
                now = datetime.now()
                timestamp = now.strftime("%Y%m%d_%H:%M:%S")

                # Create directory structure
                date_dir = now.strftime("%Y-%m-%d")
                target_dir = self.snapshot_dir / camera / date_dir
                target_dir.mkdir(parents=True, exist_ok=True)

                # Create filename
                filename = f"{timestamp}--{event_id}.mp4"
                target_path = target_dir / filename

                # Check if file already exists
                if target_path.exists():
                    self.log(f"Video clip already exists: {target_path}")
                    self._add_to_cache(cache_key, str(target_path), target_path.stat().st_size)
                    self.metrics.downloads_successful += 1
                    return f"{camera}/{date_dir}/{filename}"

                # Download video clip
                clip_url = f"{self.frigate_url}/{event_id}/clip.mp4"
                self.log(f"Downloading video clip (attempt {attempt + 1}): {clip_url}")

                response = requests.get(clip_url, timeout=self.connection_timeout)
                response.raise_for_status()

                with open(target_path, 'wb') as f:
                    f.write(response.content)

                # Calculate checksum for cache
                file_size = target_path.stat().st_size
                self._add_to_cache(cache_key, str(target_path), file_size)

                self.log(f"Video clip saved: {target_path}")
                self.metrics.downloads_successful += 1

                # Return relative path for notification
                return f"{camera}/{date_dir}/{filename}"

            except requests.RequestException as e:
                self.log(f"ERROR: Failed to download video clip (attempt {attempt + 1}): {e}", level="ERROR")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
            except Exception as e:
                self.log(f"ERROR: Unexpected error downloading video clip: {e}", level="ERROR")
                break

        self.metrics.downloads_failed += 1
        return None

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

    def _send_notifications(self, event_data: EventData, event_url: Optional[str]) -> None:
        """Send notifications to configured persons."""
        # Create timestamp
        timestamp = event_data.timestamp.strftime("%H:%M:%S")
        zone_str = ", ".join(event_data.entered_zones) if event_data.entered_zones else "No zones"

        # Build notification data
        notification_data = self._build_notification_data(event_data.camera, event_url)

        # Send to each configured person
        for person_config in self.person_configs:
            if not person_config.enabled:
                continue

            self._send_person_notification(
                person_config, event_data, zone_str, timestamp, notification_data
            )

    def _build_notification_data(self, camera: str, event_url: Optional[str]) -> Dict[str, Any]:
        """Build notification data dictionary."""
        base_data = {
            "actions": [{
                "action": "URI",
                "title": "Open Camera",
                "uri": f"homeassistant://navigate/dashboard-kameror/{camera}"
            }],
            "channel": f"frigate-{camera}",
            "importance": "high",
            "visibility": "public",
            "priority": "high",
            "ttl": 0,
            "notification_icon": self.cam_icons.get(camera, "mdi:cctv")
        }

        # Add video if available
        if event_url and self.ext_domain:
            base_data["video"] = f"{self.ext_domain}/local/frigate/{event_url}"

        return base_data

    def _send_person_notification(self, person_config: PersonConfig, event_data: EventData,
                                 zone_str: str, timestamp: str, notification_data: Dict[str, Any]) -> None:
        """Send notification to a specific person."""
        try:
            # Check camera filter
            if person_config.cameras and event_data.camera not in person_config.cameras:
                self.log(f"Skipping {person_config.name} - camera '{event_data.camera}' not in filter {person_config.cameras}")
                return

            # Check zone filter
            if person_config.zones and not any(zone in person_config.zones for zone in event_data.entered_zones):
                self.log(f"Skipping {person_config.name} - zones {event_data.entered_zones} not in filter {person_config.zones}")
                return

            # Check label filter
            if event_data.label not in person_config.labels:
                self.log(f"Skipping {person_config.name} - label '{event_data.label}' not in filter {person_config.labels}")
                self.metrics.label_filtered += 1
                return

            # Check cooldown
            cooldown_key = f"{person_config.notify}/{event_data.camera}"
            last_msg_time = time.time() - self.msg_cooldown.get(cooldown_key, 0)

            if last_msg_time < person_config.cooldown:
                self.log(f"Cooldown active for {person_config.name} ({event_data.camera}) - {person_config.cooldown - last_msg_time:.1f}s remaining")
                self.metrics.cooldown_skipped += 1
                return

            # Send notification
            title = f"{event_data.label} @ {event_data.camera}"
            message = f"{timestamp} - {zone_str} (ID: {event_data.event_id})"

            self.log(f"Sending notification to {person_config.name} ({person_config.notify})")
            self.call_service(
                f"notify/{person_config.notify}",
                title=title,
                message=message,
                data=notification_data
            )

            # Update cooldown
            self.msg_cooldown[cooldown_key] = time.time()
            self.metrics.notifications_sent += 1

        except Exception as e:
            self.log(f"ERROR: Failed to send notification to {person_config.name}: {e}", level="ERROR")
            self.metrics.errors += 1

    def _check_connection_health(self, kwargs: Dict[str, Any]) -> None:
        """Check and report connection health."""
        try:
            if hasattr(self, 'mqtt') and self.mqtt.is_client_connected():
                if self.connection_status != ConnectionStatus.CONNECTED:
                    self.connection_status = ConnectionStatus.CONNECTED
                    self.log("MQTT connection restored")
            else:
                if self.connection_status == ConnectionStatus.CONNECTED:
                    self.connection_status = ConnectionStatus.ERROR
                    self.log("WARNING: MQTT connection lost", level="WARNING")

        except Exception as e:
            self.log(f"ERROR: Connection health check failed: {e}", level="ERROR")

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
                self.log(f"Cleaned up {files_removed} old video files (older than {self.max_file_age_days} days)")

        except Exception as e:
            self.log(f"ERROR: Failed to cleanup old files: {e}", level="ERROR")

    def _cleanup_cache(self, kwargs: Dict[str, Any]) -> None:
        """Clean up expired cache entries."""
        try:
            cutoff_time = datetime.now() - timedelta(hours=self.cache_ttl_hours)
            expired_keys = []

            with self.cache_lock:
                for key, entry in self.file_cache.items():
                    if entry.timestamp < cutoff_time:
                        expired_keys.append(key)

                for key in expired_keys:
                    del self.file_cache[key]

            if expired_keys:
                self.log(f"Cleaned up {len(expired_keys)} expired cache entries")

        except Exception as e:
            self.log(f"ERROR: Failed to cleanup cache: {e}", level="ERROR")

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

        # Signal shutdown
        self.shutdown_event.set()

        # Wait for processing thread
        if self.processing_thread and self.processing_thread.is_alive():
            self.processing_thread.join(timeout=5)

        # Shutdown thread pool
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)

        self.log("Frigate Notifier terminated")

