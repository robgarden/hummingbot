#!/usr/bin/env python

from os.path import join, realpath
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from decimal import Decimal
import logging; logging.basicConfig(level=logging.ERROR)
import pandas as pd
from typing import List
import unittest
from hummingsim.backtest.backtest_market import BacktestMarket
from hummingsim.backtest.market import (
    AssetType,
    Market,
    MarketConfig,
    QuantizationParams
)
from hummingsim.backtest.mock_order_book_loader import MockOrderBookLoader
from hummingbot.core.clock import (
    Clock,
    ClockMode
)
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import (
    MarketEvent,
    TradeType,
    OrderType,
    OrderFilledEvent,
    BuyOrderCompletedEvent,
    SellOrderCompletedEvent,
    TradeFee
)
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.strategy.dev_2_perform_trade import PerformTradeStrategy
import sys; sys.path.insert(0, realpath(join(__file__, "../../")))


class PerformTradeUnitTest(unittest.TestCase):
    start: pd.Timestamp = pd.Timestamp("2019-01-01", tz="UTC")
    end: pd.Timestamp = pd.Timestamp("2019-01-01 01:00:00", tz="UTC")
    start_timestamp: float = start.timestamp()
    end_timestamp: float = end.timestamp()
    trading_pair: str = "COINALPHA-WETH"
    base_asset, quote_asset = trading_pair.split("-")
    clock_tick_size = 10

    def setUp(self):

        self.clock: Clock = Clock(ClockMode.BACKTEST, self.clock_tick_size, self.start_timestamp, self.end_timestamp)
        self.market: BacktestMarket = BacktestMarket()
        self.maker_data: MockOrderBookLoader = MockOrderBookLoader(
            trading_pair=self.trading_pair,
            base_currency=self.base_asset,
            quote_currency=self.quote_asset,
        )
        self.mid_price = 100
        self.time_delay = 15
        self.cancel_order_wait_time = 45
        self.maker_data.set_balanced_order_book(mid_price=self.mid_price, min_price=1,
                                                max_price=200, price_step_size=1, volume_step_size=10)
        self.market.add_data(self.maker_data)
        self.market.set_balance("COINALPHA", 500)
        self.market.set_balance("WETH", 5000)
        self.market.set_balance("QETH", 500)
        self.market.set_quantization_param(
            QuantizationParams(
                self.trading_pair, 6, 6, 6, 6
            )
        )

        self.market_info: MarketTradingPairTuple = MarketTradingPairTuple(
            self.market, self.trading_pair, self.base_asset, self.quote_asset
        )

        # Define strategies to test
        self.buy_mid_price_strategy: PerformTradeStrategy = PerformTradeStrategy(
            exchange=self.market,
            trading_pair=self.trading_pair,
            is_buy=True,
            spread=Decimal("3.0"),
            order_amount=Decimal("1.0"),
            price_type="mid_price"
        )
        self.sell_mid_price_strategy: PerformTradeStrategy = PerformTradeStrategy(
            exchange=self.market,
            trading_pair=self.trading_pair,
            is_buy=False,
            spread=Decimal("3.0"),
            order_amount=Decimal("1.0"),
            price_type="mid_price"
        )
        # TODO: Add tests for PerformTrade strategy that use different price types

        self.clock.add_iterator(self.market)
        self.maker_order_fill_logger: EventLogger = EventLogger()
        self.cancel_order_logger: EventLogger = EventLogger()
        self.buy_order_completed_logger: EventLogger = EventLogger()
        self.sell_order_completed_logger: EventLogger = EventLogger()

        self.market.add_listener(MarketEvent.BuyOrderCompleted, self.buy_order_completed_logger)
        self.market.add_listener(MarketEvent.SellOrderCompleted, self.sell_order_completed_logger)
        self.market.add_listener(MarketEvent.OrderFilled, self.maker_order_fill_logger)
        self.market.add_listener(MarketEvent.OrderCancelled, self.cancel_order_logger)

    @staticmethod
    def simulate_limit_order_fill(market: Market, limit_order: LimitOrder):
        quote_currency_traded: Decimal = limit_order.price * limit_order.quantity
        base_currency_traded: Decimal = limit_order.quantity
        quote_currency: str = limit_order.quote_currency
        base_currency: str = limit_order.base_currency
        config: MarketConfig = market.config

        if limit_order.is_buy:
            market.set_balance(quote_currency, market.get_balance(quote_currency) - quote_currency_traded)
            market.set_balance(base_currency, market.get_balance(base_currency) + base_currency_traded)
            market.trigger_event(MarketEvent.OrderFilled, OrderFilledEvent(
                market.current_timestamp,
                limit_order.client_order_id,
                limit_order.trading_pair,
                TradeType.BUY,
                OrderType.LIMIT,
                limit_order.price,
                limit_order.quantity,
                TradeFee(Decimal(0.0))
            ))
            market.trigger_event(MarketEvent.BuyOrderCompleted, BuyOrderCompletedEvent(
                market.current_timestamp,
                limit_order.client_order_id,
                base_currency,
                quote_currency,
                base_currency if config.buy_fees_asset is AssetType.BASE_CURRENCY else quote_currency,
                base_currency_traded,
                quote_currency_traded,
                Decimal(0.0),
                OrderType.LIMIT
            ))
        else:
            market.set_balance(quote_currency, market.get_balance(quote_currency) + quote_currency_traded)
            market.set_balance(base_currency, market.get_balance(base_currency) - base_currency_traded)
            market.trigger_event(MarketEvent.OrderFilled, OrderFilledEvent(
                market.current_timestamp,
                limit_order.client_order_id,
                limit_order.trading_pair,
                TradeType.SELL,
                OrderType.LIMIT,
                limit_order.price,
                limit_order.quantity,
                TradeFee(Decimal(0.0))
            ))
            market.trigger_event(MarketEvent.SellOrderCompleted, SellOrderCompletedEvent(
                market.current_timestamp,
                limit_order.client_order_id,
                base_currency,
                quote_currency,
                base_currency if config.sell_fees_asset is AssetType.BASE_CURRENCY else quote_currency,
                base_currency_traded,
                quote_currency_traded,
                Decimal(0.0),
                OrderType.LIMIT
            ))

    def test_limit_buy_order(self):
        self.clock.add_iterator(self.buy_mid_price_strategy)
        self.clock.backtest_til(self.start_timestamp + self.clock_tick_size + self.time_delay)

        mid_price: Decimal = self.market.get_mid_price(self.trading_pair)
        expected_order_price: Decimal = mid_price * (Decimal('1') - (Decimal("3") / Decimal("100")))

        bid_orders: List[LimitOrder] = [o for o in self.buy_mid_price_strategy.active_orders if o.is_buy]
        self.assertEqual(1, len(bid_orders))
        bid_order: LimitOrder = bid_orders[0]
        self.assertEqual(expected_order_price, bid_order.price)
        self.assertEqual(1, bid_order.quantity)

    def test_limit_sell_order(self):
        self.clock.add_iterator(self.sell_mid_price_strategy)
        self.clock.backtest_til(self.start_timestamp + self.clock_tick_size + self.time_delay)

        mid_price: Decimal = self.market.get_mid_price(self.trading_pair)
        expected_order_price: Decimal = mid_price * (Decimal('1') + (Decimal("3") / Decimal("100")))

        ask_orders: List[LimitOrder] = [o for o in self.sell_mid_price_strategy.active_orders if not o.is_buy]
        self.assertEqual(1, len(ask_orders))
        ask_order: LimitOrder = ask_orders[0]
        self.assertEqual(expected_order_price, ask_order.price)
        self.assertEqual(1, ask_order.quantity)

    # def test_order_filled_events(self):
    #     self.clock.add_iterator(self.limit_buy_strategy)
    #     self.clock.add_iterator(self.limit_sell_strategy)

    #     # test whether number of orders is one
    #     # check whether the order is sell
    #     # check whether the price is correct
    #     # check whether amount is correct
    #     self.clock.backtest_til(self.start_timestamp + self.clock_tick_size + self.time_delay)
    #     self.assertEqual(1, len(self.limit_sell_strategy.active_asks))
    #     ask_order: LimitOrder = self.limit_sell_strategy.active_asks[0][1]
    #     self.assertEqual(Decimal("101"), ask_order.price)
    #     self.assertEqual(1, ask_order.quantity)

    #     self.assertEqual(1, len(self.limit_buy_strategy.active_bids))
    #     bid_order: LimitOrder = self.limit_buy_strategy.active_bids[0][1]
    #     self.assertEqual(Decimal("99"), bid_order.price)
    #     self.assertEqual(1, bid_order.quantity)

    #     # Simulate market fill for limit buy and limit sell
    #     self.simulate_limit_order_fill(self.market, bid_order)
    #     self.simulate_limit_order_fill(self.market, ask_order)

    #     fill_events = self.maker_order_fill_logger.event_log
    #     self.assertEqual(2, len(fill_events))
    #     bid_fills: List[OrderFilledEvent] = [evt for evt in fill_events if evt.trade_type is TradeType.SELL]
    #     ask_fills: List[OrderFilledEvent] = [evt for evt in fill_events if evt.trade_type is TradeType.BUY]
    #     self.assertEqual(1, len(bid_fills))
    #     self.assertEqual(1, len(ask_fills))

    # def test_with_insufficient_balance(self):
    #     # Set base balance to zero and check if sell strategies don't place orders
    #     self.clock.add_iterator(self.limit_buy_strategy)
    #     self.clock.add_iterator(self.market_buy_strategy)
    #     self.market.set_balance("WETH", 0)
    #     end_ts = self.start_timestamp + self.clock_tick_size + self.time_delay
    #     self.clock.backtest_til(end_ts)
    #     self.assertEqual(0, len(self.limit_buy_strategy.active_bids))
    #     market_buy_events: List[BuyOrderCompletedEvent] = [t for t in self.buy_order_completed_logger.event_log
    #                                                        if isinstance(t, BuyOrderCompletedEvent)]
    #     self.assertEqual(0, len(market_buy_events))
    #     self.assertEqual(False, self.limit_buy_strategy.place_orders)
    #     self.assertEqual(False, self.market_buy_strategy.place_orders)

    #     self.clock.add_iterator(self.limit_sell_strategy)
    #     self.clock.add_iterator(self.market_sell_strategy)
    #     self.market.set_balance("COINALPHA", 0)
    #     end_ts += self.clock_tick_size + self.time_delay
    #     self.clock.backtest_til(end_ts)
    #     self.assertEqual(0, len(self.limit_sell_strategy.active_asks))
    #     market_sell_events: List[SellOrderCompletedEvent] = [t for t in self.sell_order_completed_logger.event_log
    #                                                          if isinstance(t, SellOrderCompletedEvent)]
    #     self.assertEqual(0, len(market_sell_events))
    #     self.assertEqual(False, self.limit_sell_strategy.place_orders)
    #     self.assertEqual(False, self.market_sell_strategy.place_orders)
