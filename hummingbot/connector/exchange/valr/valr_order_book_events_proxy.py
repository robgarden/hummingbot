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

    def __init__(self):
        self.order_book: Dict[str, Any] = {}
        self._diff_buffer: List[Dict[str, Any]] = []

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    def convert_order_book_format(self):
        return {
            "SequenceNumber": self.order_book["SequenceNumber"],
            "Asks": [{"price": float(a["Price"]), "quantity": sum([float(o["quantity"]) for o in a["Orders"]])} for a in
                     self.order_book["Asks"]],
            "Bids": [{"price": float(b["Price"]), "quantity": sum([float(o["quantity"]) for o in b["Orders"]])} for b in
                     self.order_book["Bids"]],
        }

    async def add_snapshot_message_to_queue(self, snapshot_message: Dict[str, Any], trading_pair: str,
                                            message_queue: asyncio.Queue):
        self.logger().info("Received snapshot message in proxy")

        data = snapshot_message["data"]
        self.order_book = {
            "Asks": [{"Price": a["Price"],
                      "Orders": [{"orderId": o["orderId"], "quantity": o["quantity"]} for o in a["Orders"]]} for
                     a in data["Asks"]],
            "Bids": [{"Price": b["Price"],
                      "Orders": [{"orderId": o["orderId"], "quantity": o["quantity"]} for o in b["Orders"]]} for
                     b in data["Bids"]],
            "SequenceNumber": data["SequenceNumber"],
            "LastChange": data["LastChange"]
        }

        calculated_checksum = self.calculate_checksum(self.order_book)
        if calculated_checksum != data["Checksum"]:
            self.logger().error("Order book snapshot checksum error")
            raise AssertionError

        order_book_message: OrderBookMessage = ValrOrderBook.snapshot_message_from_exchange(
            self.convert_order_book_format(), time.time(), {"trading_pair": trading_pair})
        message_queue.put_nowait(order_book_message)

    def add_diff_message_to_queue(self, diff_message: Dict[str, Any], trading_pair: str):
        # self.logger().info("Received diff message in proxy")
        self._diff_buffer.append(diff_message)

        if len(self.order_book):
            next_sequence_number = self.order_book["SequenceNumber"] + 1
            self.logger().info("Next sequence number: %s - Found %s" % (
                next_sequence_number, self._diff_buffer[0]["data"]["SequenceNumber"]))
            found_diff_message = [dm for dm in self._diff_buffer if
                                  dm["data"]["SequenceNumber"] == next_sequence_number]

            if found_diff_message:
                self.logger().info("Found next diff message")
                new_book = self.apply_diff(diff_message)
                self.order_book = new_book
                self._diff_buffer = [dm for dm in self._diff_buffer if
                                     dm["data"]["SequenceNumber"] > next_sequence_number]

    def calculate_checksum(self, book: Dict[str, Any]) -> int:
        def select_best_orders(side: str) -> List[any]:
            side_sorted = sorted(book[side], key=lambda s: float(s["Price"]), reverse=side == "Bids")
            retval = []
            for priceLevel in side_sorted:
                for order in priceLevel["Orders"]:
                    if len(retval) == 25:
                        return retval
                    retval.append("%s:%s" % (order["orderId"], order["quantity"]))
            return retval

        bids = select_best_orders("Bids")
        asks = select_best_orders("Asks")

        orders = []
        for i in range(25):
            try:
                orders.append(bids[i])
            finally:
                pass
            try:
                orders.append(asks[i])
            finally:
                pass

        return zlib.crc32(":".join(orders).encode("utf-8"))

    def apply_diff(self, diff_message: Dict[str, Any]) -> Dict[str, Any]:
        temp = self.order_book.copy()
        for ask in diff_message["data"]["Asks"]:
            price = ask["Price"]
            temp_ask = [a for a in temp["Asks"] if a["Price"] == price]
            if not temp_ask:
                temp["Asks"].append({
                    "Price": price,
                    "Orders": ask["Orders"]
                })
            else:
                temp_ask[0]["Orders"] = [o if o["orderId"] not in [aa["orderId"] for aa in ask["Orders"]] else
                                         [aa for aa in ask["Orders"] if o["orderId"] == aa["orderId"]][0] for o in
                                         temp_ask[0]["Orders"]]

        for bid in diff_message["data"]["Bids"]:
            price = bid["Price"]
            temp_bid = [b for b in temp["Bids"] if b["Price"] == price]
            if not temp_bid:
                temp["Bids"].append({
                    "Price": price,
                    "Orders": bid["Orders"]
                })
            else:
                temp_bid[0]["Orders"] = [o if o["orderId"] not in [bb["orderId"] for bb in bid["Orders"]] else
                                         [bb for bb in bid["Orders"] if o["orderId"] == bb["orderId"]][0] for o in
                                         temp_bid[0]["Orders"]]

        # remove 0 quantity orders
        temp["Asks"] = [{"Price": a["Price"], "Orders": [o for o in a["Orders"] if o["quantity"] != "0"]} for a in
                        temp["Asks"]]
        temp["Bids"] = [{"Price": b["Price"], "Orders": [o for o in b["Orders"] if o["quantity"] != "0"]} for b in
                        temp["Bids"]]

        # remove price bands with no orders
        temp["Asks"] = [a for a in temp["Asks"] if len(a["Orders"])]
        temp["Bids"] = [b for b in temp["Bids"] if len(b["Orders"])]
        temp["SequenceNumber"] = diff_message["data"]["SequenceNumber"]

        self.logger().info(temp)

        calculated_checksum = self.calculate_checksum(temp)
        actual_checksum = diff_message["data"]["Checksum"]

        self.logger().info("Calculated checksum: %s" % calculated_checksum)
        self.logger().info("Actual checksum: %s" % actual_checksum)
        return temp

    # def calculate_diff(self, diff_message: Dict[str, Any]) -> Dict[str, Any]:
    #     data = diff_message["data"]
    #     return {
    #         "Asks": [{"Price":} for a]
    #     }
