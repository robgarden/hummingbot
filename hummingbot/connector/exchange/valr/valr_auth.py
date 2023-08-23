import hashlib
import hmac
import json
from typing import Any, Dict, Optional

from hummingbot.connector.exchange.valr import valr_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class ValrAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        # is there a better way to do this, i.e. can we just get the endpoint path?
        path = request.url.replace(CONSTANTS.REST_URL, "")
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        headers.update(self.header_for_authentication(path, str(request.method), request.data))
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        pass

    def header_for_authentication(self, path: str, method: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
          1. Create a SHA512 HMAC hash using your API Secret and the values pertaining to your request (timestamp, HTTP verb, API path, body) detailed above.
          2. REST: Include the following headers in each request:
              X-VALR-API-KEY : Your API Key
              X-VALR-SIGNATURE : The request signature that was generated for your request (see point 1)
              X-VALR-TIMESTAMP : The same timestamp used to generate the request signature
          3. WebSocket: Pass in the same three headers to the first call that establishes the WebSocket connection. (See WebSocket API section below for details)
        """
        timestamp = int(self.time_provider.time() * 1e3)
        signature = self._generate_signature(path, method, timestamp, data)
        return {
            "X-VALR-API-KEY": self.api_key,
            "X-VALR-SIGNATURE": signature,
            "X-VALR-TIMESTAMP": str(timestamp),
        }

    def _generate_signature(self, path: str, verb: str, timestamp: int, body: Optional[Dict[str, Any]]):
        """
            Signs the request payload using the api key secret
            timestamp - the unix timestamp of this request e.g. int(time.time()*1000)
            verb - Http verb - GET, POST, PUT or DELETE
            path - path excluding host name, e.g. '/v1/withdraw
            body - http request body as a string, optional
        """

        body_string = json.dumps(body) if body else ""
        payload = "{}{}{}{}".format(timestamp, verb.upper(), path, body_string)
        return hmac.new(self.secret_key.encode('utf-8'), payload.encode("utf-8"), hashlib.sha512).hexdigest()
