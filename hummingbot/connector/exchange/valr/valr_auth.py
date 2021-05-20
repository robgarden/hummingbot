import hmac
import hashlib
import json
from typing import Dict, Any


class ValrAuth():
    """
    Auth class required by crypto.com API
    Learn more at https://exchange-docs.crypto.com/#digital-signature
    """
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key

    """Signs the request payload using the api key secret
    timestamp - the unix timestamp of this request e.g. int(time.time()*1000)
    verb - Http verb - GET, POST, PUT or DELETE
    path - path excluding host name, e.g. '/v1/withdraw
    body - http request body as a string, optional
    """
    def generate_signature(self, path: str, verb: str, timestamp: int, body: Dict[str, Any] = {}):
        bodyString = json.dumps(body) if len(body) else ""
        payload = "{}{}{}{}".format(timestamp, verb.upper(), path, bodyString)
        message = bytearray(payload, 'utf-8')
        signature = hmac.new(bytearray(self.secret_key, 'utf-8'), message, digestmod=hashlib.sha512).hexdigest()
        return signature

    def get_headers(self, signature: str, timestamp: int) -> Dict[str, Any]:
        """
        Generates authentication headers required by crypto.com
        :return: a dictionary of auth headers
        """

        # 1. Create a SHA512 HMAC hash using your API Secret and the values pertaining to your request (timestamp, HTTP verb, API path, body) detailed above.
        # 2. REST: Include the following headers in each request:
        #     X-VALR-API-KEY : Your API Key
        #     X-VALR-SIGNATURE : The request signature that was generated for your request (see point 1)
        #     X-VALR-TIMESTAMP : The same timestamp used to generate the request signature
        # 3. WebSocket: Pass in the same three headers to the first call that establishes the WebSocket connection. (See WebSocket API section below for details)

        return {
            "X-VALR-API-KEY": self.api_key,
            "X_VALR_API_KEY": self.api_key,
            "X-VALR-SIGNATURE": signature,
            "X-VALR-TIMESTAMP": str(timestamp),
            "Content-Type": 'application/json'
        }
