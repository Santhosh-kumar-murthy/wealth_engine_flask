import json

import paho.mqtt.client as mqtt
import pymysql
from pymysql.cursors import DictCursor

from controllers.logs_controller import LogsController
from controllers.settings_controller import SettingsController
from database_config import db_config


class MqttPublisher:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)

    def on_publish(self, client, userdata, mid, a, b):
        print("Message Published...", client, userdata, mid, a, b)

    def publish_payload(self, payload):
        logs_controller = LogsController()
        logs_controller.add_log(str(payload))
        settings_controller = SettingsController()
        settings = settings_controller.get_settings()
        try:
            mqtt_msg = json.dumps(payload, default=str)
            mqtt_host = settings['mqtt_host']
            mqtt_port = int(settings['mqtt_port'])
            keep_alive_interval = 45
            mqtt_topic = settings['mqtt_topic']
            mqtt_username = "wealthi_admin"
            mqtt_password = "Wealthi@#123"
            mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            mqtt_client.username_pw_set(mqtt_username, mqtt_password)
            mqtt_client.on_publish = self.on_publish
            mqtt_client.connect(mqtt_host, mqtt_port, keep_alive_interval)
            mqtt_client.publish(mqtt_topic, mqtt_msg)
            mqtt_client.disconnect()
        except Exception as exc:
            logs_controller.add_log(str(exc))
