import pyotp
from SmartApi import SmartConnect

from broker_libs.sh_api_helper import ShoonyaApiPy


def get_refresh_totp(totp_token):
    totp = pyotp.TOTP(totp_token)
    return totp.now()


kite_config = {
    'kite_client_id': "RI4984",
    'kite_password': "98453847",
    'totp_token': "ECMHGXA2QFSRMKETTY5U4WHY4EMPEI5C"
}

shoonya_config = {
    "user": "FA329812",
    "pwd": "Sandy@#123",
    "factor2": 'F462T2IC47J76N724E7PX5T3QSCHQ3O3',
    "vc": "FA329812_U",
    "app_key": "58e4edd319eb1625b9f0cf4ef6867bd5",
    "imei": "abc1234"
}

angel_one_config = {
    "api_key": "izudZxIa",
    "totp_token": "27WRYLFKTZTDMJ2JIGRQWGVRQQ",
    "client_id": "M489596",
    "password": "2580"
}

if __name__ == '__main__':
    # kite = KiteApp(enctoken=get_enctoken(kite_config['kite_client_id'], kite_config['kite_password'],
    #                                      get_refresh_totp(kite_config['totp_token'])))
    # kite.margins()
    # print(kite.profile())
    # -----------------------------------
    # obj = SmartConnect(api_key=angel_one_config['api_key'])
    # data = obj.generateSession(angel_one_config['client_id'], angel_one_config['password'],
    #                            get_refresh_totp(angel_one_config['totp_token']))
    # refreshToken = data['data']['refreshToken']
    # feedToken = obj.getfeedToken()
    # userProfile = obj.getProfile(refreshToken)
    # val = obj.getMarketData(mode="LTP", exchangeTokens={"NFO": ["35007"]})['data']['fetched'][0]['ltp']
    # print(val)
    # -----------------------------------
    #
    # api = ShoonyaApiPy()
    # ret = api.login(userid=shoonya_config['user'], password=shoonya_config['pwd'],
    #                 twoFA=get_refresh_totp(shoonya_config['factor2']), vendor_code=shoonya_config['vc'],
    #                 api_secret=shoonya_config['app_key'], imei=shoonya_config['imei'])
    # # print(ret)
    # val = api.get_quotes('NFO', '35007')['lp']
    # print(val)
    print("")
