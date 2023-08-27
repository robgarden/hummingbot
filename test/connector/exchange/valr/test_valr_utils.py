import unittest
from decimal import Decimal

from hummingbot.connector.exchange.valr.valr_utils import get_fee_currency
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder


class ValrOrderBookUtilsUnitTest(unittest.TestCase):
    def test_get_fee_currency(self):
        """
        Fee currency:
            If you are a Maker and you are Buying BTC with ZAR, your reward will be paid in ZAR
            If you are a Maker and you are Selling BTC for ZAR, your reward will be paid in BTC
            If you are a Taker and you are Buying BTC with ZAR, your fee will be charged in BTC
            If you are a Taker and you are Selling BTC for ZAR, your fee will be charged in ZAR
        """

        trading_pair = "BTC-ZAR"

        # maker
        maker_buy_order = InFlightOrder("clientOrderId", trading_pair, OrderType.LIMIT_MAKER, TradeType.BUY,
                                        Decimal("0.1"), 1)
        maker_sell_order = InFlightOrder("clientOrderId", trading_pair, OrderType.LIMIT_MAKER, TradeType.SELL,
                                         Decimal("0.1"), 1)
        self.assertEqual(get_fee_currency(maker_buy_order), "ZAR")
        self.assertEqual(get_fee_currency(maker_sell_order), "BTC")

        # taker
        taker_buy_order = InFlightOrder("clientOrderId", trading_pair, OrderType.LIMIT, TradeType.BUY, Decimal("0.1"),
                                        1)
        taker_sell_order = InFlightOrder("clientOrderId", trading_pair, OrderType.LIMIT, TradeType.SELL, Decimal("0.1"),
                                         1)
        self.assertEqual(get_fee_currency(taker_buy_order), "BTC")
        self.assertEqual(get_fee_currency(taker_sell_order), "ZAR")
