"""
This module provides encryption functionality.
"""

import base64
import hmac
import hashlib
import time


class Encryption():

    @classmethod
    def __b64_encode_decode(cls, target: str | bytes, command: str | list, en_de: bool = False) -> str:
        run_dict = {
            "85": [base64.b85encode, base64.b85decode],
            "64": [base64.b64encode, base64.b64decode],
            "32": [base64.b32encode, base64.b32decode],
            "16": [base64.b16encode, base64.b16decode],
        }

        if isinstance(target, str):
            target = bytes(target, "utf-8")
        if isinstance(command, list):
            for i in command:
                target = cls.__b64_encode_decode(target, i, en_de)
        else:
            if isinstance(command, (int, float)):
                command = str(command).split(".")[0]
            if command not in run_dict.keys():
                return target

            target = run_dict[command][en_de](target)

        if isinstance(target, bytes):
            target = target.decode("utf-8")

        return target

    @classmethod
    def __bs_hmc_encode(cls, password: str, sk: str) -> str:
        sk = bytes(sk, 'utf-8')
        password = bytes(password, 'utf-8')
        signature_hash = hmac.new(
            sk, password, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(signature_hash).decode()
        return signature

    @classmethod
    def __token_generator(cls, account: str, password: str, sk: str) -> str:
        signature = cls.__bs_hmc_encode(password, sk)
        token = "{},{},{}".format(signature, account, time.time() + 300)
        token = cls.__b64_encode_decode(target=token, command=[85, 64])
        return token

    @classmethod
    def build_token(cls) -> str:
        result = cls.__token_generator("NormalTA", "0", str(time.time()))
        return result
