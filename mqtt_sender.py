# import time
#
# from controllers.mqtt_publisher import MqttPublisher
#
# mqtt_conn = MqttPublisher()
# count = 0
# while True:
#     count = count + 1
#     mqtt_conn.publish_payload({"test": count})
#     time.sleep(120)

import requests

url = "https://sandbox.cashfree.com/verification/reverse-penny-drop"

headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "x-client-id": "CF374874CPOKH02MML80HLA01FA0",
    "x-client-secret": "cfsk_ma_test_dff80e97ff665871335028d81f249864_892a5cdb"
}
response = requests.post(url, headers=headers, data={"verification_id": "11", "name": "sandy"})

print(response.status_code)
print(response.text)
