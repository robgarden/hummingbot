#!/usr/bin/env python
import asyncio
import logging
import time
import aiohttp
import pandas as pd

from typing import Optional, List, Dict, Any
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
# from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger import HummingbotLogger

from . import valr_constants as constants
from . import valr_utils
from .valr_auth import ValrAuth
from .valr_active_order_tracker import ValrActiveOrderTracker
from .valr_order_book import ValrOrderBook
from .valr_websocket import ValrWebsocket, ValrWebSocketConnectionType
from .valr_utils import iso8601_to_s


class ValrAPIOrderBookDataSource(OrderBookTrackerDataSource):
    MAX_RETRIES = 20
    MESSAGE_TIMEOUT = 30.0
    SNAPSHOT_TIMEOUT = 10.0

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, valr_auth: ValrAuth, trading_pairs: List[str] = None):
        super().__init__(trading_pairs)
        self._trading_pairs: List[str] = trading_pairs
        self._snapshot_msg: Dict[str, any] = {}
        self._valr_auth: ValrAuth = valr_auth

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        result = {}
        async with aiohttp.ClientSession() as client:
            # https://api.valr.com/v1/public/marketsummary
            # example response:
            # [
            #     {
            #         "currencyPair": "BTCZAR",
            #         "askPrice": "10000",
            #         "bidPrice": "7005",
            #         "lastTradedPrice": "7005",
            #         "previousClosePrice": "7005",
            #         "baseVolume": "0.16065663",
            #         "highPrice": "10000",
            #         "lowPrice": "7005",
            #         "created": "2019-04-20T13:02:03.228Z",
            #         "changeFromPrevious": "0"
            #     },
            #     ...
            # ]
            resp = await client.get(f"{constants.REST_URL}/v1/public/marketsummary")
            resp_json = await resp.json()
            for t_pair in trading_pairs:
                last_trade = [float(o["lastTradedPrice"]) for o in resp_json if o["currencyPair"] ==
                              valr_utils.convert_to_exchange_trading_pair(t_pair)]
                if last_trade and last_trade[0] is not None:
                    result[t_pair] = last_trade[0]
        return result

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        async with aiohttp.ClientSession() as client:
            async with client.get(f"{constants.REST_URL}/v1/public/marketsummary", timeout=10) as response:
                if response.status == 200:
                    try:
                        resp_json = await response.json()
                        return [valr_utils.convert_from_exchange_trading_pair(item["currencyPair"]) for item in resp_json]
                    except Exception:
                        pass
                        # Do nothing if the request fails -- there will be no autocomplete for kucoin trading pairs
                return []

    @staticmethod
    async def get_order_book_data(trading_pair: str) -> Dict[str, any]:
        """
        Get whole orderbook
        """
        async with aiohttp.ClientSession() as client:
            # https://api.valr.com/v1/marketdata/:currencyPair/orderbook
            # {
            #     "Asks": [
            #         {
            #         "side": "sell",
            #         "quantity": "0.55241566",
            #         "price": "171150",
            #         "currencyPair": "BTCZAR",
            #         "orderCount": 5
            #         },
            #     ],
            #     "Bids": [
            #         {
            #         "side": "buy",
            #         "quantity": "11.00650027",
            #         "price": "171149",
            #         "currencyPair": "BTCZAR",
            #         "orderCount": 3
            #         },
            #     ],
            #     "LastChange": "2020-05-13T09:11:15.826178Z"
            # }
            url = "%s/v1/public/%s/orderbook" % (constants.REST_URL, valr_utils.convert_to_exchange_trading_pair(trading_pair))
            orderbook_response = await client.get(url)

            if orderbook_response.status != 200:
                logging.log(logging.INFO, "cannot get order book data")
                logging.log(logging.INFO, await orderbook_response.text())
                raise IOError(
                    f"Error fetching OrderBook for {trading_pair} at {constants.EXCHANGE_NAME}. "
                    f"HTTP status is {orderbook_response.status}."
                )

            # orderbook_data: List[Dict[str, Any]] = await safe_gather(orderbook_response.json())
            orderbook_data: Dict[str, any] = await orderbook_response.json()

            # orderbook_data = orderbook_data[0]
            orderbook_data = {
                "currencyPairSymbol": valr_utils.convert_to_exchange_trading_pair(trading_pair),
                "data": orderbook_data
            }

        return orderbook_data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        snapshot: Dict[str, Any] = await self.get_order_book_data(trading_pair)
        snapshot_timestamp: float = time.time()
        snapshot_msg: OrderBookMessage = ValrOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        order_book = self.order_book_create_function()
        active_order_tracker: ValrActiveOrderTracker = ValrActiveOrderTracker()
        bids, asks = active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
        order_book.apply_snapshot(bids, asks, snapshot_msg.update_id)
        return order_book

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for trades using websocket trade channel
        """
        while True:
            try:
                ws = ValrWebsocket(self._valr_auth, ValrWebSocketConnectionType.TRADE)
                await ws.connect()

                # example subscribe message
                # {
                #     "type": "SUBSCRIBE",
                #     "subscriptions": [{
                #         "event": "NEW_TRADE",
                #         "pairs": [
                #             "BTCZAR"
                #         ]
                #     }
                # }
                await ws.subscribe(list({
                    "event": "NEW_TRADE",
                    "pairs": [valr_utils.convert_to_exchange_trading_pair(pair) for pair in self._trading_pairs]
                }))

                async for response in ws.on_message():
                    # example response
                    # {
                    #     "type": "NEW_TRADE",
                    #     "currencyPairSymbol": "BTCZAR",
                    #     "data": {
                    #         "price": "9500",
                    #         "quantity": "0.001",
                    #         "currencyPair": "BTCZAR",
                    #         "tradedAt": "2019-04-25T19:51:55.393Z",
                    #         "takerSide": "buy"
                    #     }
                    # }
                    if response.get("type") != "NEW_TRADE":
                        continue

                    trade: Dict[Any] = response["data"]
                    trade_timestamp: int = iso8601_to_s(trade["tradedAt"])
                    trade_msg: OrderBookMessage = ValrOrderBook.trade_message_from_exchange(
                        trade,
                        trade_timestamp,
                        metadata={"trading_pair": valr_utils.convert_from_exchange_trading_pair(trade["currencyPair"])}
                    )
                    output.put_nowait(trade_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
            finally:
                await ws.disconnect()

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook diffs using websocket book channel
        """
        while True:
            try:
                ws = ValrWebsocket(self._valr_auth, ValrWebSocketConnectionType.TRADE)
                await ws.connect()
                await ws.subscribe(list({
                    "event": "AGGREGATED_ORDERBOOK_UPDATE",
                    "pairs": [valr_utils.convert_to_exchange_trading_pair(pair) for pair in self._trading_pairs]
                }))

                # {
                #     "type": "AGGREGATED_ORDERBOOK_UPDATE",
                #     "currencyPairSymbol": "BTCZAR",
                #     "data": {
                #         "Asks": [
                #             {
                #                 "side": "sell",
                #                 "quantity": "0.005",
                #                 "price": "9500",
                #                 "currencyPair": "BTCZAR",
                #                 "orderCount": 1
                #             },
                #             ...
                #         ],
                #         "Bids": [...],
                #         "LastChange": "2020-06-01T11:54:51.634Z"
                #     }
                # }
                async for response in ws.on_message():
                    if response.get("type") != "AGGREGATED_ORDERBOOK_UPDATE":
                        continue

                    timestamp: int = iso8601_to_s(response["data"]["LastChange"])
                    # data in this channel is not order book diff but the entire order book (up to depth 20).
                    # so we need to convert it into a order book snapshot.
                    # VARL does not offer order book diff ws updates.
                    orderbook_msg: OrderBookMessage = ValrOrderBook.snapshot_message_from_exchange(
                        response,
                        timestamp,
                        metadata={"trading_pair": valr_utils.convert_from_exchange_trading_pair(response["data"]["currencyPairSymbol"])}
                    )
                    output.put_nowait(orderbook_msg)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().network(
                    "Unexpected error with WebSocket connection.",
                    exc_info=True,
                    app_warning_msg="Unexpected error with WebSocket connection. Retrying in 30 seconds. "
                                    "Check network connection."
                )
                self.logger().log(20, e)
                await asyncio.sleep(30.0)
            finally:
                await ws.disconnect()

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        """
        Listen for orderbook snapshots by fetching orderbook
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot: Dict[str, any] = await self.get_order_book_data(trading_pair)
                        snapshot_timestamp: int = iso8601_to_s(snapshot["data"]["LastChange"])
                        snapshot_msg: OrderBookMessage = ValrOrderBook.snapshot_message_from_exchange(
                            snapshot,
                            snapshot_timestamp,
                            metadata={"trading_pair": trading_pair}
                        )
                        output.put_nowait(snapshot_msg)
                        self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                        # Be careful not to go above API rate limits.
                        await asyncio.sleep(5.0)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        self.logger().network(
                            "Unexpected error with WebSocket connection.",
                            exc_info=True,
                            app_warning_msg="Unexpected error with WebSocket connection. Retrying in 5 seconds. "
                                            "Check network connection."
                        )
                        self.logger().log(20, e)
                        await asyncio.sleep(5.0)
                this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                delta: float = next_hour.timestamp() - time.time()
                await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().log(20, e)
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
