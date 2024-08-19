# Import package
import json

import paho.mqtt.client as mqtt

# Define Variables
MQTT_HOST = "192.168.0.118"
MQTT_PORT = 1883
MQTT_KEEPALIVE_INTERVAL = 45
MQTT_TOPIC = "helloTopic"
MQTT_MSG = "hello MQTT"


# Define on connect event function
# We shall subscribe to our Topic in this function
def on_connect(mosq, obj, rc, a, b):
    mqttc.subscribe(MQTT_TOPIC, 0)


# Define on_message event function.
# This function will be invoked every time,
# a new message arrives for the subscribed topic
def on_message(mosq, obj, msg):
    trade = json.loads(msg.payload.decode('utf-8'))
    print(trade)

def on_subscribe(mosq, obj, mid, granted_qos, c):
    print("Subscribed to Topic: " + MQTT_MSG + " with QoS: " + str(granted_qos))


# Initiate MQTT Client
mqttc = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe

# Connect with MQTT Broker
mqttc.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)

# Continue monitoring the incoming messages for subscribed topic
mqttc.loop_forever()