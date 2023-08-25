import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from hummingbot.connector.exchange.valr import valr_constants as CONSTANTS
from hummingbot.connector.exchange.valr.valr_order_book import ValrOrderBook
from hummingbot.connector.exchange.valr.valr_order_book_utils import apply_diff, calculate_checksum
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.logger import HummingbotLogger


class ValrOrderBookEventsProxy():
    _logger: Optional[HummingbotLogger] = None

    """
        order book with price bands as keys
    """
    _order_book: Dict[str, Any] = {}
    _diff_buffer: List[Dict[str, Any]] = []

    def __init__(self):
        pass

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    def _convert_order_book_format(self):
        """
            formatted_order_book with array format
        """

        def format_price_bands(side: str):
            bands = []
            for order in self._order_book[side]:
                band = {
                    "price": float(order["Price"]),
                    "quantity": sum([float(o["quantity"]) for o in order["Orders"]]),
                }
                bands.append(band)
            return bands

        return {
            "SequenceNumber": self._order_book["SequenceNumber"],
            "Bids": format_price_bands("Bids"),
            "Asks": format_price_bands("Asks"),
        }

    async def process_message(self, message: Dict[str, Any], trading_pair: str, snapshot_queue: asyncio.Queue):
        # "router"
        # could be a snapshot or a diff message
        event_type = message.get("type")
        if event_type == CONSTANTS.FULL_ORDERBOOK_SNAPSHOT_EVENT_TYPE:
            self.logger().debug("Received snapshot message in proxy")
            await self._process_snapshot(message)
        elif event_type == CONSTANTS.FULL_ORDERBOOK_UPDATE_EVENT_TYPE:
            # TODO: send diff updates on diff queue instead of snapshots
            self.logger().debug("Received diff message in proxy")
            await self._process_diff(message)

        await self._convert_order_book_and_send_to_snapshot_queue(trading_pair, snapshot_queue)

    async def _process_snapshot(self, snapshot_message: Dict[str, Any]):
        # parse snapshot message and assigns it to internal order book
        order_book_data = snapshot_message["data"]
        new_order_book = order_book_data

        calculated_checksum = calculate_checksum(new_order_book)
        expected_checksum = new_order_book["Checksum"]
        if calculated_checksum != expected_checksum:
            self.logger().error("Order book snapshot checksum error")
            raise Exception("Calculated checksum=%s - Expected checksum=%s" % (calculated_checksum, expected_checksum))

        self._order_book = new_order_book

    async def _convert_order_book_and_send_to_snapshot_queue(self, trading_pair: str, snapshot_queue: asyncio.Queue):
        # formats order book correctly for hb and puts it on the snapshot queue
        formatted_order_book = self._convert_order_book_format()
        order_book_message: OrderBookMessage = ValrOrderBook.snapshot_message_from_exchange(
            formatted_order_book, time.time(), {"trading_pair": trading_pair})
        # await snapshot_queue.put(order_book_message)
        snapshot_queue.put_nowait(order_book_message)

    async def _process_diff(self, diff_message: Dict[str, Any]):
        if not len(self._order_book):
            # can't process diff without full order book
            return

        self._validate_sequence_number(diff_message)

        diff_data = diff_message["data"]
        new_book = apply_diff(self._order_book, diff_data)

        calculated_checksum = calculate_checksum(new_book)
        expected_checksum = diff_message["data"]["Checksum"]

        if calculated_checksum != expected_checksum:
            raise Exception("Calculated checksum=%s - Expected checksum=%s" % (calculated_checksum, expected_checksum))

        new_book["SequenceNumber"] = diff_data["SequenceNumber"]
        self._order_book = new_book

    def _validate_sequence_number(self, diff_message: Dict[str, Any]):
        def count_orders_on_side(side: str) -> int:
            bands = diff_message["data"][side]
            return sum([len(band["Orders"]) for band in bands])

        if self._order_book:
            current_sequence_number = self._order_book["SequenceNumber"]
            diff_sequence_number = diff_message["data"]["SequenceNumber"]
            num_orders = sum([count_orders_on_side(side) for side in ["Bids", "Asks"]])
            expected_sequence_number = current_sequence_number + num_orders
            if diff_sequence_number != expected_sequence_number:
                raise Exception(
                    "Invalid sequence number %s, expected %s" % (diff_sequence_number, expected_sequence_number))
