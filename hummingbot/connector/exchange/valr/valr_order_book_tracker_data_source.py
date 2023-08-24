import asyncio
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.valr import valr_constants as CONSTANTS, valr_web_utils as web_utils
from hummingbot.connector.exchange.valr.valr_order_book import ValrOrderBook
from hummingbot.connector.exchange.valr.valr_order_book_events_proxy import ValrOrderBookEventsProxy
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.valr.valr_exchange import ValrExchange


class ValrOrderBookTrackerDataSource(OrderBookTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'ValrExchange',
                 api_factory: WebAssistantsFactory):
        super().__init__(trading_pairs)
        self._connector = connector
        self._trade_messages_queue_key = CONSTANTS.NEW_TRADE_EVENT_TYPE
        self._diff_messages_queue_key = CONSTANTS.FULL_ORDERBOOK_UPDATE_EVENT_TYPE
        self._snapshot_messages_queue_key = CONSTANTS.FULL_ORDERBOOK_SNAPSHOT_EVENT_TYPE
        self._api_factory = api_factory
        self._valr_order_book_events_proxy = ValrOrderBookEventsProxy()

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.
        :param trading_pair: the trading pair for which the order book will be retrieved
        :return: the response from the exchange (JSON dictionary)
        """
        rest_assistant = await self._api_factory.get_rest_assistant()
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.ORDER_BOOK_FULL_PATH_URL % symbol),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDER_BOOK_FULL_PATH_URL,
            is_auth_required=True,
        )

        return data

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """

        self.logger().info("Subscribing to trade events")

        try:
            trade_subscription = {
                "event": CONSTANTS.NEW_TRADE_EVENT_TYPE,
                "pairs": [],
            }
            order_book_subscription = {
                "event": CONSTANTS.FULL_ORDERBOOK_UPDATE_EVENT_TYPE,
                "pairs": [],
            }
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                trade_subscription["pairs"].append(symbol.upper())
                order_book_subscription["pairs"].append(symbol.upper())
            payload = {
                "type": "SUBSCRIBE",
                "subscriptions": [trade_subscription, order_book_subscription]
            }
            subscribe_request: WSJSONRequest = WSJSONRequest(payload=payload)

            await ws.send(subscribe_request)

            self.logger().info("Subscribed to public order book and trade events...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WSS_TRADE_URL, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        # with open("open-book-snapshot-rest.txt", "w") as f:
        #     f.write(json.dumps(snapshot))
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = ValrOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair, "source": "rest"}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["currencyPairSymbol"])
        trade_message = ValrOrderBook.trade_message_from_exchange(
            raw_message, {"trading_pair": trading_pair})
        message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["currencyPairSymbol"])
        self._valr_order_book_events_proxy.add_diff_message_to_queue(raw_message, trading_pair)
        # order_book_message: OrderBookMessage = ValrOrderBook.diff_message_from_exchange(
        #     raw_message, time.time(), {"trading_pair": trading_pair})
        # message_queue.put_nowait(order_book_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Create an instance of OrderBookMessage of type OrderBookMessageType.SNAPSHOT

        :param raw_message: the JSON dictionary of the public trade event
        :param message_queue: queue where the parsed messages should be stored in
        """

        """
            example:
            {
              "type": "FULL_ORDERBOOK_SNAPSHOT",
              "currencyPairSymbol": "BTCZAR",
              "data": {
                "LastChange": 1692810053438,
                "Asks": [
                  {
                    "Price": "496141",
                    "Orders": [
                      {
                        "orderId": "9fa3d56d-6b67-4085-bdd9-b6f37132ac81",
                        "quantity": "0.14185685"
                      }
                    ]
                  },
                  {
                    "Price": "496142",
                    "Orders": [
                      {
                        "orderId": "98c96fd0-fe6f-4afd-94ed-0c00c9fe39bb",
                        "quantity": "0.07582513"
                      }
                    ]
                  },
                ],
                "SequenceNumber": 123213,
                "Checksum": 1231313,
              },
            }
        """
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=raw_message["currencyPairSymbol"])
        await self._valr_order_book_events_proxy.add_snapshot_message_to_queue(raw_message, trading_pair, message_queue)
        # order_book_message: OrderBookMessage = ValrOrderBook.snapshot_message_from_exchange(
        #     raw_message["data"], time.time(), {"trading_pair": trading_pair, "source": "ws"})
        # message_queue.put_nowait(order_book_message)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        event_type = event_message.get("type")

        if event_type == CONSTANTS.FULL_ORDERBOOK_SNAPSHOT_EVENT_TYPE:
            return self._snapshot_messages_queue_key

        if event_type == CONSTANTS.FULL_ORDERBOOK_UPDATE_EVENT_TYPE:
            return self._diff_messages_queue_key

        if event_type == CONSTANTS.NEW_TRADE_EVENT_TYPE:
            return self._trade_messages_queue_key

        return ""
