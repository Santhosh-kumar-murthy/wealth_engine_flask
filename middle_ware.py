import json

import paho.mqtt.client as mqtt
from alice_blue import TransactionType, OrderType, ProductType
from paho.mqtt.enums import CallbackAPIVersion
from pya3 import Aliceblue

broker_address = '150.242.200.220'  # Replace with your VPS IP or domain
broker_port = 1883  # Default MQTT port
mqtt_username = "wealthi_admin"  # Replace with your MQTT username
mqtt_password = "Wealthi@#123"  # Replace with your MQTT password
topic = "wealthi/getPayload"  # The topic to subscribe to or publish to

users = [
    {
        "name": "Santhosh",
        "alice_user_name": "1424639",
        "password": "Pooja@#123",
        "twoFA": "DWJMCLHGFLZXULDUCSUJOZETCCLJBMGD",
        'APIKey': 'Fb4i97HmB6q7TXMFHHDb9yWrYGBdaQ24LcV4sQZuoFFydUkVVHZDo0BNSTnGpRtoxZ5rlLIBqqz7Yn0pJ2DvYDEmTFFgpKCUDbpE1Yb9pAGhYsnbvsr2pqQbCu1EvyWa',
        "app_id": "ESvpTgyyVTWEUgg",
        "api_secret": "ZdziFPkPaYPIxHLOHslgflZetXxsIyTvCSVipsvsNoJcXmFZDCSBOvExmotMFFkayRMrjHBOgevsNIvNSddDsvesUzVZXyAtbvkg",
        "opt_buy": True,
        "opt_sell": True,
        "fut": False
    },
]


def placeOrder(instrument, position_type, user, transaction_type):
    print(instrument)
    print(position_type)
    print(transaction_type)
    alice = Aliceblue(user_id=user['alice_user_name'], api_key=user['APIKey'])
    print(alice.get_session_id())  # Get Session ID
    print(alice.get_profile())
    res = alice.place_order(
        transaction_type=transaction_type,
        instrument=alice.get_instrument_by_symbol(instrument['alice_exchange'], instrument['alice_trading_symbol']),
        quantity=1,
        order_type=OrderType.Market,
        product_type=ProductType.Delivery,
        price=0.0,
        trigger_price=None,
        stop_loss=None,
        square_off=None,
        trailing_sl=None,
        is_amo=False,
        order_tag='order1'
    )
    print("order response", res)


def decideOrders(alice_fut, alice_buy_option, alice_sell_opt, position_type):
    for user in users:
        if user['fut']:
            placeOrder(alice_fut, position_type, user, TransactionType.Buy)
        if user['opt_buy']:
            placeOrder(alice_buy_option, position_type, user, TransactionType.Buy)
        if user['opt_sell']:
            placeOrder(alice_sell_opt, position_type, user, TransactionType.Sell)


def on_message(client_data, userdata, message):
    try:
        payload = message.payload.decode('utf-8')
        decoded_payload = json.loads(payload)
        if isinstance(decoded_payload, str):
            decoded_payload = json.loads(decoded_payload)
        if isinstance(decoded_payload, dict):
            if decoded_payload['trade_type'] == 'entry':
                fut_position = decoded_payload['fut_trade']
                position_type = decoded_payload['position_type']
                alice_fut = fut_position['instrument']['alice_blue']
                fut_current_price = fut_position['fut_current_price']
                alice_buy_option = decoded_payload['opt_buy']['buy_option_data']['alice_option']
                buy_option_current_price = decoded_payload['opt_buy']['buy_option_current_price']
                alice_sell_opt = decoded_payload['opt_sell']['sell_option_data']['alice_option']
                sell_option_current_price = decoded_payload['opt_sell']['sell_option_current_price']
                decideOrders(alice_fut, alice_buy_option, alice_sell_opt, position_type)
    except json.JSONDecodeError as e:
        print(e)


client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
client.username_pw_set(mqtt_username, mqtt_password)
client.on_message = on_message
client.connect(broker_address, broker_port)
client.subscribe(topic)
client.loop_forever()
