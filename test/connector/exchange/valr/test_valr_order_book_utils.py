import json
import logging
import os
import unittest
from typing import Dict

from hummingbot.connector.exchange.valr.valr_order_book_utils import apply_diff, calculate_checksum


def order(order_id: int, quantity: int) -> Dict[str, str]:
    return {"orderId": str(order_id), "quantity": str(quantity)}


class ValrOrderBookUtilsUnitTest(unittest.TestCase):

    def test_apply_diff(self):
        """
        Tests if applying a diff to an order book results in the correct new order_book
        """
        order_book = {
            "Asks": [
                {"Price": "200", "Orders": [order(5, 7)]},
                {"Price": "300", "Orders": [order(1, 1), order(2, 2), order(3, 3)]},
                {"Price": "500", "Orders": [order(9, 1), order(8, 2)]},
            ],
            "Bids": [
                {"Price": "200", "Orders": [order(1, 10)]},
                {"Price": "100", "Orders": [order(1, 5), order(2, 10), order(3, 20)]},
            ],
        }

        diff = {
            "Asks": [
                {"Price": "300", "Orders": [order(1, 0), order(2, 0), order(3, 0)]},
                {"Price": "400", "Orders": [order(4, 1)]},
                {"Price": "500", "Orders": [order(9, 0), order(8, 1)]},
            ],
            "Bids": [
                {"Price": "150", "Orders": [order(4, 1)]},
                {"Price": "100", "Orders": [order(1, 4), order(2, 0)]},
            ],
        }

        new_order_book = apply_diff(order_book, diff)

        expected_order_book = {
            "Asks": [
                {"Price": "200", "Orders": [order(5, 7)]},
                {"Price": "400", "Orders": [order(4, 1)]},
                {"Price": "500", "Orders": [order(8, 1)]},
            ],
            "Bids": [
                {"Price": "200", "Orders": [order(1, 10)]},
                {"Price": "150", "Orders": [order(4, 1)]},
                {"Price": "100", "Orders": [order(1, 4), order(3, 20)]},
            ],
        }

        self.assertTrue(new_order_book == expected_order_book)

    def test_apply_diff2(self):
        order_book = {
            "Bids": [
                {"Price": "200", "Orders": [order(1, 10)]},
                {"Price": "100", "Orders": [order(1, 5), order(2, 10), order(3, 20)]},
            ],
            "Asks": [
                {"Price": "200", "Orders": [order(5, 7)]},
                {"Price": "300", "Orders": [order(1, 1), order(2, 2), order(3, 3)]},
                {"Price": "500", "Orders": [order(9, 1), order(8, 2)]},
            ]
        }

        diff = {
            "Bids": [],
            "Asks": [
                {"Price": "300", "Orders": [order(1, 0), order(2, 0), order(3, 0)]},
            ]
        }

        new_order_book = apply_diff(order_book, diff)

        expected_order_book = {
            "Bids": [
                {"Price": "200", "Orders": [order(1, 10)]},
                {"Price": "100", "Orders": [order(1, 5), order(2, 10), order(3, 20)]},
            ],
            "Asks": [
                {"Price": "200", "Orders": [order(5, 7)]},
                {"Price": "500", "Orders": [order(9, 1), order(8, 2)]},
            ]
        }

        self.assertTrue(new_order_book == expected_order_book)

    def test_checksum(self):
        with open('__fixtures__/FULL_ORDERBOOK_SNAPSHOT-1652084266.json', 'r') as f:
            snapshot_message = json.loads(f.read())
            order_book = snapshot_message["data"]
            expected_checksum = order_book["Checksum"]
            actual_checksum = calculate_checksum(order_book)
            self.assertEqual(actual_checksum, expected_checksum)

    def test_applying_multiple_diffs(self):
        # snapshot_message = {}
        with open("__fixtures__/FULL_ORDERBOOK_SNAPSHOT-1652084266.json", 'r') as f:
            snapshot_message = json.loads(f.read())
        diffs = []
        diff_filenames = [file_name for file_name in os.listdir("__fixtures__") if "UPDATE" in file_name]
        for diff_filename in diff_filenames:
            with open("__fixtures__/%s" % diff_filename) as f:
                diffs.append(json.loads(f.read())["data"])

        order_book = snapshot_message["data"]
        diffs = sorted(diffs, key=lambda d: d["SequenceNumber"])
        for diff in diffs:
            order_book = apply_diff(order_book, diff)
            calculated_checksum = calculate_checksum(order_book)
            expected_checksum = diff["Checksum"]
            self.assertEqual(calculated_checksum, expected_checksum)


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
