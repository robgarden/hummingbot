#!/usr/bin/env python
import asyncio
# import copy
import logging
import websockets
import ujson
from enum import Enum
from hummingbot.core.utils.async_utils import safe_ensure_future

from typing import Optional, AsyncIterable, Any, List, Dict
from websockets.exceptions import ConnectionClosed
from hummingbot.logger import HummingbotLogger

from . import valr_constants as constants
from .valr_auth import ValrAuth
from .valr_utils import RequestId, get_ms_timestamp


class ValrWebSocketConnectionType(Enum):
    ACCOUNT = 1
    TRADE = 2

    def valr_websocket_path(self):
        if self == ValrWebSocketConnectionType.ACCOUNT:
            return "/ws/account"
        else:
            return "/ws/trade"


class ValrWebsocket(RequestId):
    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, auth: ValrAuth, connection_type: ValrWebSocketConnectionType):
        self._auth: ValrAuth = auth
        self._connection_type: ValrWebSocketConnectionType = connection_type
        self._client: Optional[websockets.WebSocketClientProtocol] = None

    # connect to exchange
    async def connect(self):
        try:
            # authenticate using headers
            path = self._connection_type.valr_websocket_path()
            timestamp = get_ms_timestamp()
            signature = self._auth.generate_signature(path, "get", timestamp)
            extra_headers = self._auth.get_headers(signature, timestamp)
            ws_url = "%s%s" % (constants.WSS_URL, path)
            self._client = await websockets.connect(ws_url, extra_headers=extra_headers)

            return self._client
        except Exception as e:
            self.logger().error(f"Websocket error: '{str(e)}'", exc_info=True)

    # disconnect from exchange
    async def disconnect(self):
        if self._client is None:
            return

        await self._client.close()

    # receive & parse messages
    async def _messages(self) -> AsyncIterable[Any]:
        try:
            while True:
                try:
                    raw_msg_str: str = await asyncio.wait_for(self._client.recv(), timeout=self.MESSAGE_TIMEOUT)
                    raw_msg = ujson.loads(raw_msg_str)
                    if "method" in raw_msg and raw_msg["method"] == "public/heartbeat":
                        payload = {"id": raw_msg["id"], "method": "public/respond-heartbeat"}
                        safe_ensure_future(self._client.send(ujson.dumps(payload)))
                    yield raw_msg
                except asyncio.TimeoutError:
                    await asyncio.wait_for(self._client.ping(), timeout=self.PING_TIMEOUT)
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await self.disconnect()

    # emit messages
    async def _emit(self, data: Optional[Any] = {}):
        # logging.log(logging.INFO, "Web socket emit")
        # logging.log(logging.INFO, data)
        return await self._client.send(ujson.dumps(data))

    # request via websocket
    async def request(self, data: Optional[Any] = {}):
        return await self._emit(data)

    # subscribe to a method
    async def subscribe(self, subscriptions: List[Dict[str, any]]) -> int:
        return await self.request({
            "type": "SUBSCRIBE",
            "subscriptions": subscriptions
        })

    # unsubscribe to a method
    async def unsubscribe(self, subscriptions: List[Dict[str, any]]) -> int:
        return await self.request({
            "type": "SUBSCRIBE",
            "subscriptions": subscriptions
        })

    # listen to messages by method
    async def on_message(self) -> AsyncIterable[Any]:
        async for msg in self._messages():
            yield msg
