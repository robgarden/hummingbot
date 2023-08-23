import asyncio
from typing import TYPE_CHECKING, List, Optional

from hummingbot.connector.exchange.valr import valr_constants as CONSTANTS
from hummingbot.connector.exchange.valr.valr_auth import ValrAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.valr.valr_exchange import ValrExchange


class ValrUserStreamTrackerDataSource(UserStreamTrackerDataSource):

    LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800  # Recommended to Ping/Update listen key to keep connection alive
    HEARTBEAT_TIME_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: ValrAuth,
                 trading_pairs: List[str],
                 connector: 'ValrExchange',
                 api_factory: WebAssistantsFactory):
        super().__init__()
        self._auth: ValrAuth = auth
        self._current_listen_key = None
        self._api_factory = api_factory

        self._listen_key_initialized_event: asyncio.Event = asyncio.Event()
        self._last_listen_key_ping_ts = 0

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """

        ws: WSAssistant = await self._get_ws_assistant()
        headers = self._auth.header_for_authentication(CONSTANTS.WSS_ACCOUNT_PATH, "GET")
        await ws.connect(ws_url=CONSTANTS.WSS_ACCOUNT_URL, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL, ws_headers=headers)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to account balance update events through the provided websocket connection.
        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:
            balance_update_subscription = {
                "event": CONSTANTS.BALANCE_UPDATE_EVENT_TYPE
            }
            payload = {
                "type": "SUBSCRIBE",
                "subscriptions": [balance_update_subscription]
            }
            subscribe_request: WSJSONRequest = WSJSONRequest(payload=payload)

            await websocket_assistant.send(subscribe_request)

            self.logger().info("Subscribed to balance update events...")
        except asyncio.CancelledError as e:
            self.logger().info(e)
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to balance update events...",
                exc_info=True
            )
            raise

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant
