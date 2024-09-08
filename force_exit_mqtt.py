import json

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from broker_libs.broker_methods import get_kite_broker, get_angel_broker, get_shoonya_broker
from controllers.data_signals_controller import DataSignalsController
from controllers.mqtt_publisher import MqttPublisher
from controllers.positions_controller import PositionsController
from controllers.settings_controller import MqttSettingsController

settings_controller = MqttSettingsController()
settings = settings_controller.get_settings()
mqtt_host = settings['mqtt_host']
mqtt_port = int(settings['mqtt_port'])
keep_alive_interval = 45
mqtt_topic = settings['mqtt_topic']
mqtt_username = "wealthi_admin"
mqtt_password = "Wealthi@#123"
positions_controller = PositionsController()


def on_message(client, userdata, message):
    data_signal_controller = DataSignalsController()
    select_broker = {
        1: get_kite_broker,
        2: get_angel_broker,
        3: get_shoonya_broker,
    }
    payload = message.payload.decode('utf-8')
    try:
        decoded_payload = json.loads(payload)
        if isinstance(decoded_payload, str):
            decoded_payload = json.loads(decoded_payload)
        if isinstance(decoded_payload, dict):
            if decoded_payload.get('type') == 'force_exit':
                mqtt_publisher = MqttPublisher()
                active_system_use_broker = data_signal_controller.get_active_broker()
                broker_id = active_system_use_broker['broker_id']
                config = json.loads(active_system_use_broker['broker_config_params'])
                broker = select_broker.get(broker_id)(config)
                position = positions_controller.get_a_position(decoded_payload.get('pos_id'))
                status, message, exit_payload = positions_controller.exit_existing_position(existing_position=position,
                                                                                            broker_id=broker_id,
                                                                                            broker=broker)
                print(exit_payload)
                mqtt_publisher.publish_payload(exit_payload)

    except json.JSONDecodeError as e:
        print(e)


client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
client.username_pw_set(mqtt_username, mqtt_password)
client.on_message = on_message
client.connect(mqtt_host, mqtt_port)
client.subscribe(mqtt_topic)
client.loop_forever()
