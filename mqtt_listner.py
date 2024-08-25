import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

# Define the MQTT broker details
broker_address = '150.242.200.220'  # Replace with your VPS IP or domain
broker_port = 1883  # Default MQTT port
mqtt_username = "wealthi_admin"  # Replace with your MQTT username
mqtt_password = "Wealthi@#123"  # Replace with your MQTT password
topic = "wealthi/getPayload"  # The topic to subscribe to or publish to


# The callback function to be called when the client receives a message
def on_message(client, userdata, message):
    print(f"Received message: {str(message.payload.decode('utf-8'))} on topic {message.topic}")


# Create a new MQTT client instance
client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)

# Set the username and password for the connection
client.username_pw_set(mqtt_username, mqtt_password)

# Attach the callback function to the client
client.on_message = on_message

# Connect to the broker
client.connect(broker_address, broker_port)

# Subscribe to a topic
client.subscribe(topic)

# Start the loop to process network traffic, dispatch callbacks, and handle reconnecting
client.loop_forever()
