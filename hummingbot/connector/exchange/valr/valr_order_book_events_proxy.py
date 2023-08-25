import asyncio
import logging
import time
import zlib
from typing import Any, Dict, List, Optional

from hummingbot.connector.exchange.valr.valr_order_book import ValrOrderBook
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

    def convert_order_book_format(self):
        """
            formatted_order_book with array format
        """

        def format_price_bands(side: str):
            bands = []
            for price, orders in self._order_book[side].items():
                band = {
                    "price": float(price),
                    "quantity": sum([float(o["quantity"]) for o in orders]),
                }
                bands.append(band)
            return bands

        return {
            "SequenceNumber": self._order_book["SequenceNumber"],
            "Bids": format_price_bands("Bids"),
            "Asks": format_price_bands("Asks"),
        }

    async def add_snapshot_message_to_queue(self, snapshot_message: Dict[str, Any], trading_pair: str,
                                            message_queue: asyncio.Queue):
        def parse_side(side: str, data: Dict[str, Any]) -> Dict[str, Any]:
            side_dict = {}
            for band in data[side]:
                side_dict[band["Price"]] = band["Orders"]
            return side_dict

        self.logger().info("Received snapshot message in proxy")

        data = snapshot_message["data"]
        new_order_book = {
            "SequenceNumber": data["SequenceNumber"],
            "Bids": parse_side("Bids", data),
            "Asks": parse_side("Asks", data),
        }

        calculated_checksum = self.calculate_checksum(new_order_book)
        if calculated_checksum != data["Checksum"]:
            self.logger().error("Order book snapshot checksum error")
            raise AssertionError

        self._order_book = new_order_book

        order_book_message: OrderBookMessage = ValrOrderBook.snapshot_message_from_exchange(
            self.convert_order_book_format(), time.time(), {"trading_pair": trading_pair})
        message_queue.put_nowait(order_book_message)

    async def add_diff_message_to_queue(self, diff_message: Dict[str, Any], trading_pair: str, queue: asyncio.Queue):
        # self.logger().info("Received diff message in proxy")
        self._diff_buffer.append(diff_message)

        if len(self._order_book):
            next_sequence_number = self._order_book["SequenceNumber"] + 1

            # drop all diffs earlier than the current book
            self._diff_buffer = [dm for dm in self._diff_buffer if
                                 dm["data"]["SequenceNumber"] >= next_sequence_number]

            while True:
                if len(self._diff_buffer):
                    first_diff = self._diff_buffer[0]
                    first_diff_sequence_number = first_diff["data"]["SequenceNumber"]
                    self.logger().info("Next sequence number=%s - First diff=%s" % (
                        next_sequence_number, first_diff_sequence_number))

                    if not first_diff_sequence_number == next_sequence_number:
                        return

                    new_book = self.apply_diff(first_diff)

                    calculated_checksum = self.calculate_checksum(new_book)
                    expected_checksum = diff_message["data"]["Checksum"]

                    self.logger().info(
                        "Calculated checksum=%s - Expected checksum=%s" % (calculated_checksum, expected_checksum))

                    self._order_book = new_book
                    self._diff_buffer = self._diff_buffer[1:]

    @staticmethod
    def calculate_checksum(book: Dict[str, Any]) -> int:
        def select_best_orders(side: str) -> List[any]:
            bands = [{"Price": price, "Orders": orders} for price, orders in book[side].items()]
            bands_sorted = sorted(bands, key=lambda s: float(s["Price"]), reverse=side == "Bids")
            retval = []
            for priceLevel in bands_sorted:
                for order in priceLevel["Orders"]:
                    if len(retval) == 25:
                        return retval
                    retval.append("%s:%s" % (order["orderId"], order["quantity"]))
            return retval

        bids = select_best_orders("Bids")
        asks = select_best_orders("Asks")

        orders_str = []
        for i in range(25):
            try:
                orders_str.append(bids[i])
            finally:
                pass
            try:
                orders_str.append(asks[i])
            finally:
                pass

        return zlib.crc32(":".join(orders_str).encode("utf-8"))

    def apply_diff(self, diff_message: Dict[str, Any]) -> Dict[str, Any]:
        def parse_side(side: str, data: Dict[str, Any]) -> Dict[str, Any]:
            side_dict = {}
            for band in data[side]:
                side_dict[band["Price"]] = band["Orders"]
            return side_dict

        def calculate_new_side(side: str, book: Dict[str, Any]) -> Dict[str, Any]:
            new_side = {}
            parsed_diff_side = parse_side(side, diff_message["data"])
            for price, orders in book[side].items():
                new_orders = []
                diff_orders = parsed_diff_side.get(price, [])

                for o in orders:
                    updated_order = ([do for do in diff_orders if do["orderId"] == o["orderId"]] or [o])[0]
                    if updated_order["quantity"] != "0":
                        new_orders.append(
                            {"orderId": updated_order["orderId"], "quantity": updated_order["quantity"]})

                if new_orders:
                    new_side[price] = new_orders
            return new_side

        return {
            "SequenceNumber": diff_message["data"]["SequenceNumber"],
            "Bids": calculate_new_side("Bids", self._order_book),
            "Asks": calculate_new_side("Asks", self._order_book),
        }
