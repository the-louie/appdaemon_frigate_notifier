import pytz
from datetime import datetime, timedelta
import time
import json
import appdaemon.plugins.hass.hassapi as hass
import mqttapi as mqtt

timezone = pytz.timezone('Europe/Stockholm')

"""
doorbell_notify:
  module: i1_frigate_notifier
  class: FrigateNotification
  base_url: "https://aycyqh1aurhmtfazoifltrigtwivfefc.ui.nabu.casa/api/frigate/notifications"
  persons:
    - name: anders
      notify: mobile_app_louies_telefon
      tracker: device_tracker.louies_telefon
      cooldown: 600
  only_zones: True
"""

DBG=False

class FrigateNotification(hass.Hass):
  def initialize(self):
    self.mqtt = self.get_plugin_api("MQTT")
    self.mqtt.mqtt_subscribe('frigate/events/#')

    self.topic = self.args.get("topic")
    self.persons = self.args.get("persons", [])
    self.base_url = self.args.get("hass_base_url")
    self.event_url = self.args.get("event_url")
    self.only_zones = self.args.get("only_zones", False)

    self.msg_cooldown = {}

    if self.mqtt.is_client_connected():
      self.log('MQTT is connected')
      self.mqtt.listen_event(self.motion, "MQTT_MESSAGE")
      #self.mqtt.listen_event(self.motion, "MQTT_MESSAGE", topic="frigate/events/#" )


  def motion(self, event_name, data, kwargs):
    if not (data.get('topic','')).startswith(self.topic):
        return
    payload = json.loads(data.get("payload"))
    if payload is None:
      return
    event_data = payload.get("after", {})
    event_type = payload.get("type", "")
    event_id = event_data.get("id")
    camera = event_data.get("camera", "Unknown")
    label = event_data.get("label", "Unknown")
    entered_zones = event_data.get("entered_zones", [])

    if event_id is None:
      self.log("Unkown mqtt event (event.id is None)")
      return

    if event_type != "end":
      if DBG:
        self.log("event_type is '{}' not 'end'".format(event_type))
      return

 #   if DBG:
 #       self.log(json.dumps(data))
    if self.only_zones and not entered_zones:
      if DBG:
        self.log("entered_zones is false ({}) and only_zones is {}".format(','.join(entered_zones), self.only_zones))
      return

    if DBG:
      self.log("EVENT: {} @{} zones: {} (only: {})".format(event_id, camera, ','.join(entered_zones), self.only_zones))

    self.notify(label, camera, event_id)

    

  def notify(self, label, camera, event_id):
    if (self.base_url is not None):
      #notification_data = {"image": "{}/{}/snapshot.jpg".format(self.base_url, event_id), "video": "{}/{}/clip.mp4".format(self.base_url, event_id)}
      notification_data = {"image": "{}/{}/snapshot.jpg".format(self.base_url, event_id), "actions":[{"action": "URI", "title": "Clip", "uri": "{}/{}/index.m3u8".format(self.event_url, event_id)}] }
      self.log("frigate notify: {}".format(notification_data))
    else:
      notification_data = {}

    for person in self.persons:
      self.log("notify> {}".format(person.get("notify")))
      self.log("cooldown for {} = {} (min={})".format(person.get("notify"), (time.time() - self.msg_cooldown.get(person.get("notify"), 0)), int(person.get("cooldown"))))
      if time.time() - self.msg_cooldown.get(person.get("notify"), 0) < int(person.get("cooldown")):
        self.log("cooldown activated for {}, last msg sent {}s ago".format(person.get("notify"), time.time() - self.msg_cooldown.get(person.get("notify"), 0)), level="DEBUG")
      else:
        self.call_service("notify/{}".format(person.get("notify")), message="{} {}".format(label, camera), data=notification_data)
        self.msg_cooldown[person.get("notify")] = time.time()
        self.log("notify/{}".format(person.get("notify")), level="DEBUG")

