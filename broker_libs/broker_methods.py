import datetime
import time

import pyotp
from SmartApi import SmartConnect

from broker_libs.kite_trade import KiteApp, get_enctoken
from broker_libs.sh_api_helper import ShoonyaApiPy


def get_refresh_totp(totp_token_value):
    totp = pyotp.TOTP(totp_token_value)
    return totp.now()


def get_kite_broker(broker_config):
    kite = KiteApp(enctoken=get_enctoken(broker_config['kite_client_id'], broker_config['kite_password'],
                                         get_refresh_totp(broker_config['totp_token'])))
    return kite


def get_angel_broker(broker_config):
    smart_api = SmartConnect(api_key=broker_config['api_key'])
    session_data = smart_api.generateSession(broker_config['client_id'], broker_config['password'],
                                             get_refresh_totp(broker_config['totp_token']))
    refresh_token = session_data['data']['refreshToken']
    smart_api.generateToken(refresh_token)
    return smart_api


def get_shoonya_broker(broker_config):
    shoonya_api = ShoonyaApiPy()
    shoonya_api.login(userid=broker_config['user'], password=broker_config['pwd'],
                      twoFA=get_refresh_totp(broker_config['factor2']), vendor_code=broker_config['vc'],
                      api_secret=broker_config['app_key'],
                      imei=broker_config['imei'])
    return shoonya_api


def get_ltp_shoonya(broker, instrument):
    val = broker.get_quotes('NFO', instrument['shoonya_token'])['lp']
    return val


def get_ltp_angel(broker, instrument):
    val = broker.getMarketData(
        mode="LTP",
        exchangeTokens={"NFO": [instrument['angel_token']]}
    )['data']['fetched'][0]['ltp']
    return val


def get_ltp_zerodha(broker, instrument):
    from_datetime = datetime.datetime.now() - datetime.timedelta(days=5)
    to_datetime = datetime.datetime.now()
    candle_data = broker.historical_data(
        instrument['zerodha_instrument_token'],
        from_datetime,
        to_datetime,
        '5minute',
    )
    time.sleep(0.5)
    return candle_data[-1]['close']
