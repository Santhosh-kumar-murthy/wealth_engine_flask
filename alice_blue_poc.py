import base64

import pyotp
from Crypto import Random
from Crypto.Cipher import AES
from pya3 import *


class CryptoJsAES:
    @staticmethod
    def __pad(data):
        block_size = 16
        length = block_size - (len(data) % block_size)
        return data + (chr(length) * length).encode()

    @staticmethod
    def __unpad(data):
        return data[:-(data[-1] if type(data[-1]) == int else ord(data[-1]))]

    @staticmethod
    def __bytes_to_key(data, salt, output=48):
        assert len(salt) == 8, len(salt)
        data += salt
        key = hashlib.md5(data).digest()
        final_key = key
        while len(final_key) < output:
            key = hashlib.md5(key + data).digest()
            final_key += key
        return final_key[:output]

    @staticmethod
    def encrypt(message, passphrase):
        salt = Random.new().read(8)
        key_iv = CryptoJsAES.__bytes_to_key(passphrase, salt, 32 + 16)
        key = key_iv[:32]
        iv = key_iv[32:]
        aes = AES.new(key, AES.MODE_CBC, iv)
        return base64.b64encode(b"Salted__" + salt + aes.encrypt(CryptoJsAES.__pad(message)))

    @staticmethod
    def decrypt(encrypted, passphrase):
        encrypted = base64.b64decode(encrypted)
        assert encrypted[0:8] == b"Salted__"
        salt = encrypted[8:16]
        key_iv = CryptoJsAES.__bytes_to_key(passphrase, salt, 32 + 16)
        key = key_iv[:32]
        iv = key_iv[32:]
        aes = AES.new(key, AES.MODE_CBC, iv)
        return CryptoJsAES.__unpad(aes.decrypt(encrypted[16:]))


BASE_URL = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService"
#
#
# INI_FILE = "ab_options_sell.ini"              # Set .ini file name used for storing config info.
# # Load parameters from the config file
# cfg = configparser.ConfigParser()
# cfg.read(INI_FILE)

alice_obj = {
    "username": "1424639",
    "password": "Pooja@#123",
    'APIKey': 'Fb4i97HmB6q7TXMFHHDb9yWrYGBdaQ24LcV4sQZuoFFydUkVVHZDo0BNSTnGpRtoxZ5rlLIBqqz7Yn0pJ2DvYDEmTFFgpKCUDbpE1Yb9pAGhYsnbvsr2pqQbCu1EvyWa',
    "twoFA": "DWJMCLHGFLZXULDUCSUJOZETCCLJBMGD",
    "app_id": "ESvpTgyyVTWEUgg",
    "api_secret": "ZdziFPkPaYPIxHLOHslgflZetXxsIyTvCSVipsvsNoJcXmFZDCSBOvExmotMFFkayRMrjHBOgevsNIvNSddDsvesUzVZXyAtbvkg"
}

userId = alice_obj['username']
password = alice_obj['password']
twofa = '1997'
totp_encrypt_key = alice_obj['twoFA']

totp = pyotp.TOTP(totp_encrypt_key)
url = BASE_URL + "/customer/getEncryptionKey"

payload = json.dumps({
    "userId": userId
})
headers = {
    'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload, verify=True)

encKey = response.json()["encKey"]

checksum = CryptoJsAES.encrypt(password.encode(), encKey.encode()).decode('UTF-8')

url = BASE_URL + "/customer/webLogin"

payload = json.dumps({
    "userId": userId,
    "userData": checksum
})
headers = {
    'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload, verify=True)

response_data = response.json()

url = BASE_URL + "/sso/2fa"

payload = json.dumps({
    "answer1": twofa,
    "userId": userId,
    "sCount": str(response_data['sCount']),
    "sIndex": response_data['sIndex']
})
headers = {
    'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload, verify=True)

if response.json()["loPreference"] == "TOTP" and response.json()["totpAvailable"]:
    url = BASE_URL + "/sso/verifyTotp"

    payload = json.dumps({
        "tOtp": totp.now(),
        "userId": userId
    })

    headers = {
        'Authorization': 'Bearer ' + userId + ' ' + response.json()['us'],
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload, verify=True)

    print(response.json())
if response.json()["userSessionID"]:
    print("Login Successfully")
else:
    print("User is not TOTP enabled! Please enable TOTP through mobile or web")

alice = Aliceblue(user_id=alice_obj['username'], api_key=alice_obj['APIKey'])
print(alice.get_session_id())  # Get Session ID
print(alice.get_profile())  # get profile
