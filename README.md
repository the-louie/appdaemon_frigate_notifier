# Frigate Notification App for AppDaemon

Copyright (c) 2025 the_louie

A sophisticated AppDaemon application that listens to Frigate MQTT events and sends intelligent notifications to configured users when motion is detected. This app supports zone filtering, cooldown periods, video clip downloads, and customizable notification settings with enterprise-grade reliability and performance.

## Features

- **MQTT Integration**: Listens to Frigate events via MQTT
- **Zone Filtering**: Only notify when objects enter specific zones
- **Label Filtering**: Filter notifications by detected object types (person, car, etc.)
- **Camera Filtering**: Filter notifications by specific cameras
- **Cooldown System**: Prevent notification spam with configurable cooldown periods
- **Video Downloads**: Automatically download video clips for notifications
- **Multi-User Support**: Send notifications to multiple users with different preferences
- **Custom Icons**: Set custom icons for different cameras
- **Error Handling**: Robust error handling and logging
- **Type Safety**: Full type hints for better code maintainability
- **Async Processing**: Non-blocking event processing with thread pools
- **Retry Logic**: Automatic retry for failed downloads
- **File Cleanup**: Automatic cleanup of old video files
- **Metrics Tracking**: Comprehensive metrics for monitoring
- **Memory Management**: Efficient memory usage with structured data
- **Intelligent Caching**: Prevents duplicate downloads with TTL-based cache
- **Connection Monitoring**: Real-time MQTT connection health tracking
- **Event Queue**: Efficient event processing queue

## Installation

1. Copy `i1_frigate_notifier.py` to your AppDaemon `apps` directory
2. Copy the configuration to your AppDaemon `conf` directory
3. Restart AppDaemon

## Configuration

### Basic Configuration

```yaml
frigate_notify:
  module: i1_frigate_notifier
  class: FrigateNotification
  mqtt_topic: "frigate/events"
  frigate_url: "https://your-frigate-instance.com/api/events"
  ext_domain: "https://your-external-domain.com"
  snapshot_dir: "/path/to/snapshots"
  only_zones: true
```

### Advanced Settings

```yaml
  # Advanced Settings
  max_file_age_days: 30    # Days to keep video files before cleanup
  enable_metrics: true     # Enable metrics logging
  cache_ttl_hours: 24      # Hours to keep cache entries
  connection_timeout: 30   # Connection timeout in seconds
```

### Person Configuration

```yaml
  persons:
    - name: "Primary User"
      notify: "mobile_app_primary_phone"
      labels:
        - "person"
        - "car"
        - "truck"
      cooldown: 120  # 2 minutes
      enabled: true     # Enable/disable notifications for this user
      zones:
        - "driveway"
        - "front_door"
      cameras:
        - "doorbell_cam"
        - "driveway_cam"

    - name: "Secondary User"
      notify: "mobile_app_secondary_phone"
      labels:
        - "person"
      cooldown: 300  # 5 minutes
      enabled: true
      # No zone/camera filters - will receive all person events
```

### Camera Icons

```yaml
  cam_icons:
    doorbell_cam: "mdi:doorbell-video"
    driveway_cam: "mdi:car-estate"
    backyard_cam: "mdi:home-group"
```

## Configuration Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `mqtt_topic` | string | No | MQTT topic to listen to (default: "frigate/events") |
| `frigate_url` | string | Yes | Base URL for Frigate API |
| `ext_domain` | string | Yes | External domain for notification links |
| `snapshot_dir` | string | No | Directory to save video clips |
| `only_zones` | boolean | No | Only notify when objects enter zones (default: false) |

| `max_file_age_days` | integer | No | Days to keep files (default: 30) |
| `enable_metrics` | boolean | No | Enable metrics logging (default: true) |
| `cache_ttl_hours` | integer | No | Cache time-to-live in hours (default: 24) |
| `connection_timeout` | integer | No | Network timeout in seconds (default: 30) |
| `persons` | list | Yes | List of users to notify |
| `cam_icons` | dict | No | Custom icons for cameras |

### Person Configuration

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | Yes | User's name (for logging) |
| `notify` | string | Yes | Home Assistant notify service |
| `labels` | list | Yes | List of object labels to notify about |
| `cooldown` | integer | No | Cooldown period in seconds (default: 0) |
| `enabled` | boolean | No | Enable/disable notifications (default: true) |
| `zones` | list | No | Specific zones to monitor (optional) |
| `cameras` | list | No | Specific cameras to monitor (optional) |

## How It Works

1. **Event Detection**: The app listens to MQTT messages from Frigate
2. **Event Processing**: Only processes 'end' events to avoid duplicate notifications
3. **Event Processing**: Events are queued for efficient processing
4. **Zone Filtering**: If `only_zones` is enabled, skips events without zone entries
5. **Media Download**: Downloads video clips with 30s timeout and 2s retries, falls back to snapshots
6. **User Filtering**: Checks each user's preferences (labels, zones, cameras, cooldown, priority)
7. **Notification**: Sends rich notifications with video links and camera shortcuts
8. **Cleanup**: Automatically removes old video files and cache entries
9. **Monitoring**: Tracks performance, connection health, and usage statistics

## Advanced Features



### Event Queue Management
- **Thread-Safe Queue**: Event queue with proper synchronization
- **Queue Overflow Protection**: Drops events when queue is full
- **Dedicated Processing Thread**: Separate thread for event processing
- **Graceful Shutdown**: Proper cleanup of threads and resources

### Intelligent Caching
- **File Cache**: Prevents duplicate downloads with TTL-based cache
- **Checksum Validation**: Ensures file integrity
- **Cache Cleanup**: Automatic cleanup of expired cache entries
- **Memory Efficient**: Thread-safe cache with proper locking

### Connection Health Monitoring
- **Real-time Status**: Tracks MQTT connection status (connected, disconnected, error)
- **Automatic Recovery**: Detects and reports connection issues
- **Health Checks**: Periodic connection validation every 5 minutes
- **Status Reporting**: Connection status included in metrics

### Async Processing
- Events are processed asynchronously to prevent blocking
- Video downloads use a thread pool (5 workers) for concurrent processing
- Non-blocking MQTT message handling

### Media Download Logic
- Video downloads with 30-second timeout and 2-second retries
- Automatic fallback to snapshot images if video fails
- Immediate notification sending when media is available

### File Management
- Automatic cleanup of old video files
- Configurable retention period
- Prevents disk space issues

### Metrics Tracking
- Comprehensive metrics for monitoring
- Event processing statistics
- Download success/failure rates
- Cooldown and filtering statistics
- Queue size and processing time tracking

### Enhanced Filtering
- Filter by specific zones per user
- Filter by specific cameras per user
- Efficient set-based lookups for performance

## Notification Features

- **Rich Content**: Includes video clips when available
- **Quick Actions**: Direct links to camera dashboards
- **Channel Grouping**: Notifications grouped by camera
- **Priority Settings**: High priority notifications with custom icons
- **Cooldown Management**: Prevents notification spam per user/camera combination
- **Granular Control**: Different settings per user for maximum flexibility
- **User Enable/Disable**: Toggle notifications per user

## File Structure

```
/snapshots/
├── doorbell_cam/
│   └── 2024-01-15/
│       └── 20240115_14:30:25--event_id.mp4
├── driveway_cam/
│   └── 2024-01-15/
│       └── 20240115_14:35:10--event_id.mp4
└── ...
```

## Performance Optimizations

- **Structured Data**: Uses dataclasses for efficient data handling
- **Set-based Lookups**: Fast filtering with sets instead of lists
- **Thread Pool**: Concurrent video downloads (5 workers)
- **Memory Management**: Efficient memory usage with proper cleanup
- **Async Processing**: Non-blocking event handling
- **Intelligent Caching**: Reduces redundant downloads

## Monitoring and Metrics

The app provides comprehensive metrics that are logged hourly:

```
METRICS: Events=150, Processed=145, Notifications=89,
Downloads=67/3, CooldownSkipped=23, LabelFiltered=12,
ZoneFiltered=8, Errors=2, QueueSize=5,
AvgProcessingTime=0.125s, Connection=connected
```

### Metrics Explained
- **Events**: Total MQTT events received
- **Processed**: Events that passed initial validation
- **Notifications**: Notifications actually sent
- **Downloads**: Successful/failed video downloads
- **CooldownSkipped**: Events skipped due to cooldown
- **LabelFiltered**: Events filtered by label preferences
- **ZoneFiltered**: Events filtered by zone settings
- **Errors**: Total errors encountered
- **QueueSize**: Current event queue size
- **AvgProcessingTime**: Average time to process events
- **Connection**: Current MQTT connection status

## Troubleshooting

### Common Issues

1. **No notifications received**
   - Check MQTT connection
   - Verify label filters match detected objects
   - Check cooldown settings
   - Verify zone/camera filters
   - Check if user is enabled

2. **Video downloads failing**
   - Verify `frigate_url` is accessible
   - Check `snapshot_dir` permissions
   - Ensure sufficient disk space
   - Verify connection timeout

3. **MQTT connection issues**
   - Verify MQTT plugin is configured in AppDaemon
   - Check topic configuration matches Frigate
   - Monitor connection health metrics

4. **High memory usage**
   - Check metrics for error accumulation
   - Verify file cleanup is working
   - Monitor thread pool usage
   - Check cache size and cleanup

5. **Event processing delays**
   - Check queue size in metrics
   - Verify thread pool is not saturated
   - Monitor average processing time

### Logging

The app provides detailed logging at different levels:
- `INFO`: Normal operation messages
- `WARNING`: Configuration issues
- `ERROR`: Operation failures
- `METRICS`: Performance statistics

### Performance Tuning

- **Thread Pool Size**: Default is 5 workers, adjust based on system resources

- **File Retention**: Adjust `max_file_age_days` based on storage capacity
- **Cache TTL**: Adjust `cache_ttl_hours` based on usage patterns
- **Cooldown Periods**: Balance between responsiveness and notification spam
- **Queue Size**: Monitor queue size to prevent event loss

## Dependencies

- AppDaemon 4.x
- MQTT Plugin for AppDaemon
- Python `requests` library
- Frigate instance with MQTT enabled

## License

This project is open source and available under the BSD 2-Clause License. See the [LICENSE](LICENSE) file for details.
