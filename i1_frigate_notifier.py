import appdaemon.plugins.hass.hassapi as hass
import pytz
from datetime import datetime, timedelta
import time
import json
import mqttapi as mqtt
import requests
import os

timezone = pytz.timezone('Europe/Stockholm')

"""
doorbell_notify:
  module: i1_frigate_notifier
  class: FrigateNotification
  base_url: "https://aycyqh1aurhmtfazoifltrigtwivfefc.ui.nabu.casa/api/frigate/notifications"
  ext_domain: "https://aycyqh1aurhmtfazoifltrigtwivfefc.ui.nabu.casa"
  event_url: "https://frigate.i.louie.se/vod/event/"
  persons:
    - name: anders
      notify: mobile_app_pixel_9_pro
      tracker: device_tracker.pixel_9_pro
      cooldown: 600
  only_zones: True
  snapshot_dir: "/mnt/storage/frigate_snapshots_appdaemon"
"""


DBG=True

class FrigateNotification(hass.Hass):
  def initialize(self):
    self.mqtt = self.get_plugin_api("MQTT")
    self.mqtt.mqtt_subscribe('frigate/events/#')

    self.topic = self.args.get("topic")
    self.persons = self.args.get("persons", [])
    self.base_url = self.args.get("hass_base_url")
    self.ext_domain = self.args.get("ext_domain")
    self.frigate_url = self.args.get("frigate_url")
    self.event_url = self.args.get("event_url")
    self.only_zones = self.args.get("only_zones", False)
    self.cam_icons = self.args.get("cam_icons", {})

    self.snapshot_dir = self.args.get("snapshot_dir")

    self.msg_cooldown = {}

    if self.mqtt.is_client_connected():
      self.log('MQTT is connected')
      self.mqtt.listen_event(self.motion, "MQTT_MESSAGE")
      #self.mqtt.listen_event(self.motion, "MQTT_MESSAGE", topic="frigate/events/#" )

    notify_destination="mobile_app_pixel_9_pro"
    # f"notify/{notify_addr}", title=camera, message=f"Rörelse {timestr} ({event_id})"
    #self.call_service(f"notify/mobile_app_pixel_9_pro", message="started...", title="frigate_notifier", data={"video": "https://aycyqh1aurhmtfazoifltrigtwivfefc.ui.nabu.casa/local/frigate/driveway_cam/2025-01-09/20250109_17:41:59--1736440904.902579-w1htlu.mp4", "actions": [{"action": "URI", "title": "Open camera", "uri": "homeassistant://navigate/dashboard-kameror/driveway_cam"}], "channel": "frigate-driveway_cam", "visibility": "public", "priority": "high", "ttl": 0, "notification_icon": "mdi:car-estate"} )
    #self.notify("test", title="titel", name=notify)

  def motion(self, event_name, data, kwargs):
    if data is not None and not data.get('topic','').startswith(self.topic):
        return
    payload = data.get("payload")
    if payload is None or not isinstance(payload, str):
      return
    payload = json.loads(payload)
    event_data = payload.get("after", {})
    event_type = payload.get("type", "")
    event_id = event_data.get("id")
    camera = event_data.get("camera", "Unknown")
    label = event_data.get("label", "Unknown")
    entered_zones = event_data.get("entered_zones", [])
    zone_str = ",".join(entered_zones)

    if self.only_zones and len(entered_zones) == 0:
      return

    if event_id is None:
      self.log("Unkown mqtt event (event.id is None)")
      return

    if event_type != "end":
      if DBG:
        self.log(f"{camera} {label} ({zone_str}) event_type is '{event_type}' not 'end'")
      return
    elif DBG:
      self.log("event_type is '{}'".format(event_type))

    if DBG:
      self.log("EVENT: {} @{} zones: {} (only_zones: {})".format(event_id, camera, ','.join(entered_zones), self.only_zones))

    event_url = self.download_snap(label, camera, event_id)
    self.send_notification(label, zone_str, camera, event_id, event_url)

  def download_snap(self, label, camera, event_id):
    if (self.base_url is None):
      return
    if (self.snapshot_dir is None):
      return
    if DBG:
      self.log(f"download_snap(self, {label}, {camera}, {event_id})")

    now = datetime.now()
    year = now.year
    month = now.month if now.month >= 10 else f"0{now.month}"
    day = now.day if now.day >= 10 else f"0{now.day}"
    hour = now.hour if now.hour >= 10 else f"0{now.hour}"
    minute = now.minute if now.minute >= 10 else f"0{now.minute}"
    second = now.second if now.second >= 10 else f"0{now.second}"
    timestr = f"{hour}:{minute}:{second}"
    
    self.log(f"*** TIME *** download_snap(): {timestr}")

    target_path = f"{self.snapshot_dir}/{camera}/{year}-{month}-{day}"
    target_filename = f"{year}{month}{day}_{hour}:{minute}:{second}--{event_id}"

    if not os.path.exists(target_path):
      os.makedirs(target_path)

    self.log(f"download_snap(self, {label}, {camera}, {event_id})")
#    try:
#        self.log(f" > downloading '{self.frigate_url}/{event_id}/snapshot.jpg -> {target_path}/{target_filename}.jpg")
#        urllib.request.urlretrieve(f"{self.frigate_url}/{event_id}/snapshot.jpg", f"{target_path}/{target_filename}.jpg")
#    except Exception as error:
#      self.log(f"EXCEPTION: Exception when fetcing {self.frigate_url}/{event_id}/snapshot.jpg")
#      self.log(f"EXCEPTION: {error}") 
#      return None

    try:
      self.log(f" > downloading '{self.frigate_url}/{event_id}/clip.mp4 -> {target_path}/{target_filename}.mp4")
      clip = requests.get(f"{self.frigate_url}/{event_id}/clip.mp4").content
      with open(f"{target_path}/{target_filename}.mp4", 'wb') as clip_file:
        clip_file.write(clip)

    except Exception as error:
      self.log(f"EXCEPTION: Exception when fetcing {self.frigate_url}/{event_id}/clip.mp4")
      self.log(f"EXCEPTION: {error}") 
      return None

    return f"{camera}/{year}-{month}-{day}/{target_filename}"


  def send_notification(self, label, zone_str, camera, event_id, event_url):
    if DBG:
      self.log("fsend_notification(self, {label}, {zone_str}, {camera}, {event_id}, {event_url})")

    now = datetime.now()
    year = now.year
    month = now.month if now.month >= 10 else f"0{now.month}"
    day = now.day if now.day >= 10 else f"0{now.day}"
    hour = now.hour if now.hour >= 10 else f"0{now.hour}"
    minute = now.minute if now.minute >= 10 else f"0{now.minute}"
    second = now.second if now.second >= 10 else f"0{now.second}"
    timestr = f"{hour}:{minute}:{second}"
    self.log(f"*** TIME *** send_notification(): {timestr}")

    if (self.base_url is not None):
      #notification_data = {"image": "{}/{}/snapshot.jpg".format(self.base_url, event_id), "video": "{}/{}/clip.mp4".format(self.base_url, event_id)}
      #notification_data = {"image": "{}/{}/snapshot.jpg".format(self.base_url, event_id), "actions":[{"action": "URI", "title": "Clip", "uri": "{}/{}/index.m3u8".format(self.event_url, event_id)}] }
      # ok ! # notification_data = {"image": f"{self.ext_domain}/local/frigate/{event_url}.jpg", "video": f"{self.ext_domain}/local/frigate/{event_url}.mp4", "actions":[{"action": "URI", "title": "Clip", "uri": f"{self.ext_domain}/local/frigate/{event_url}.mp4"}] }
      if event_url:
        notification_data = {"video": f"{self.ext_domain}/local/frigate/{event_url}.mp4", "actions":[{"action": "URI", "title": "Open camera", "uri": f"homeassistant://navigate/dashboard-kameror/{camera}"}], "channel": f"frigate-{camera}", "importance": "high", "visibility": "public", "priority": "high", "ttl": 0, "notification_icon": self.cam_icons.get(camera, "mdi:cctv") }
      else:
        notification_data = {"actions":[{"action": "URI", "title": camera, "uri": f"homeassistant://navigate/dashboard-kameror/{camera}"}], "channel": f"frigate-{camera}", "importance": "high", "visibility": "public", "notification_icon": self.cam_icons.get(camera, "mdi:cctv"), "priority": "high", "ttl": 0  }
      self.log("frigate notify: {}".format(notification_data))
    else:
      notification_data = {}

    for person in self.persons:
      notify_addr=person.get("notify")
      labels_filter=person.get("labels")
      if label not in labels_filter:
        self.log(f"Skipping label {label}, not in filter")
        continue

      last_msg_id = f"{notify_addr}/{camera}" 
      last_msg_time = (time.time() - self.msg_cooldown.get(last_msg_id, 0))

      self.log(f"notify> {notify_addr} ({label}/{camera}) id:{event_id}")
      self.log(json.dumps(notification_data))
      self.log("lastmsg for {} = {} (min={})".format(last_msg_id, last_msg_time, int(person.get("cooldown"))))

      if last_msg_time < int(person.get("cooldown")):
        self.log(f"cooldown activated for {last_msg_id}, last msg sent {last_msg_time}s ago", level="DEBUG")
      else:
        self.log(f"Sending notification to: notify/{notify_addr}")
        self.call_service(f"notify/{notify_addr}", title=f"{label}/{zone_str} @ {camera}", message=f"{timestr} ({event_id})", data=notification_data)
        self.msg_cooldown[last_msg_id] = time.time()

