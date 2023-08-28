import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from _decimal import Decimal
from bidict import bidict
from dydx3.helpers.request_helpers import iso_to_epoch_seconds

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.valr import valr_constants as CONSTANTS, valr_utils, valr_web_utils as web_utils
from hummingbot.connector.exchange.valr.valr_auth import ValrAuth
from hummingbot.connector.exchange.valr.valr_order_book_tracker_data_source import ValrOrderBookTrackerDataSource
from hummingbot.connector.exchange.valr.valr_user_stream_tracker_data_source import ValrUserStreamTrackerDataSource
from hummingbot.connector.exchange.valr.valr_web_utils import build_api_factory
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter


class ValrExchange(ExchangePyBase):
    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 valr_api_key: str,
                 valr_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 ):
        self.api_key = valr_api_key
        self.secret_key = valr_api_secret
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
        super().__init__(client_config_map)

    @property
    def name(self) -> str:
        return 'valr'

    @property
    def authenticator(self) -> AuthBase:
        return ValrAuth(
            self.api_key,
            self.secret_key,
            self._time_synchronizer,
        )

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return ""

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.PAIRS_PATH_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.PAIRS_PATH_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        pass

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        pass

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        data = {
            "customerOrderId": order_id,
            "pair": symbol,
        }

        try:
            await self._api_delete(
                path_url=CONSTANTS.ORDERS_CANCEL_V2_PATH_URL,
                data=data,
                is_auth_required=True,
                limit_id=CONSTANTS.ORDERS_CANCEL_V2_PATH_URL)
            return True
        except Exception as e:
            self.logger().warning("Error cancelling order %s: %s" % (order_id, e))

        return False

    async def _place_order(self, order_id: str, trading_pair: str, amount: Decimal, trade_type: TradeType,
                           order_type: OrderType, price: Decimal, **kwargs) -> Tuple[str, float]:
        amount_str = f"{amount:f}"
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        data = {
            "side": side_str,
            "pair": symbol,
            "customerOrderId": order_id,
        }

        if order_type is OrderType.LIMIT or order_type is OrderType.LIMIT_MAKER:
            price_str = f"{price:f}"
            data.update({
                "quantity": amount_str,
                "price": price_str,
                "timeInForce": CONSTANTS.TIME_IN_FORCE_GTC,
                "postOnly": order_type is OrderType.LIMIT,
            })

        if order_type == OrderType.MARKET:
            data.update({
                "baseAmount": amount_str,
            })

        path_url = CONSTANTS.ORDERS_MARKET_PATH_URL if order_type == OrderType.MARKET else CONSTANTS.ORDERS_LIMIT_PATH_URL
        try:
            order_result = await self._api_post(
                path_url=path_url,
                data=data,
                is_auth_required=True)
            o_id = str(order_result["id"])
            transact_time = self._time_synchronizer.time()
        except IOError:
            # error_description = str(e)
            # is_server_overloaded = ("status is 503" in error_description
            #                         and "Unknown error, please check your request or try again later." in error_description)
            # if is_server_overloaded:
            #     o_id = "UNKNOWN"
            #     transact_time = self._time_synchronizer.time()
            # else:
            raise
        return o_id, transact_time

    def _get_fee(self, base_currency: str, quote_currency: str, order_type: OrderType, order_side: TradeType,
                 amount: Decimal, price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = is_maker is True
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _update_trading_fees(self):
        pass

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type")
                if event_type == CONSTANTS.BALANCE_UPDATE_EVENT_TYPE:
                    balance_entry = event_message.get("data")
                    asset_name = balance_entry["currency"]["shortName"]
                    available_balance = Decimal(balance_entry["available"])
                    total_balance = Decimal(balance_entry["total"])
                    self._account_available_balances[asset_name] = available_balance
                    self._account_balances[asset_name] = total_balance

                elif event_type == CONSTANTS.ORDER_STATUS_UPDATE_EVENT_TYPE:
                    data = event_message.get("data")
                    client_order_id = data["customerOrderId"]
                    tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                    if tracked_order is not None:
                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=iso_to_epoch_seconds(data["orderUpdatedAt"]),
                            new_state=CONSTANTS.ORDER_STATE[data["orderStatusType"]],
                            client_order_id=client_order_id,
                            exchange_order_id=data["orderId"],
                        )
                        self._order_tracker.process_order_update(order_update=order_update)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _format_trading_rules(self, exchange_info_dict: List[Dict[str, Any]]) -> List[TradingRule]:
        # TODO: figure the correct relation between VALRs values and HB expected values
        trading_pair_rules = exchange_info_dict
        rules = []
        for rule in filter(valr_utils.is_exchange_information_valid, trading_pair_rules):
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule.get("symbol"))
                min_order_size = Decimal(rule.get("minBaseAmount"))
                min_order_value = Decimal(rule.get("minQuoteAmount"))
                tick_size = rule.get("tickSize")

                rules.append(
                    TradingRule(trading_pair,
                                min_order_size=min_order_size,
                                min_order_value=min_order_value,
                                min_price_increment=Decimal(tick_size),
                                min_base_amount_increment=min_order_size)
                )

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return rules

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        balances = await self._api_get(
            path_url=CONSTANTS.BALANCES_PATH_URL,
            is_auth_required=True,
            limit_id=CONSTANTS.BALANCES_PATH_URL)

        for balance_entry in balances:
            asset_name = balance_entry["currency"]
            available_balance = Decimal(balance_entry["available"])
            total_balance = Decimal(balance_entry["total"])
            self._account_available_balances[asset_name] = available_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        TODO: filled quote amount and fees
        Fee currency:
            If you are a Maker and you are Buying BTC with ZAR, your reward will be paid in ZAR
            If you are a Maker and you are Selling BTC for ZAR, your reward will be paid in BTC
            If you are a Taker and you are Buying BTC with ZAR, your fee will be charged in BTC
            If you are a Taker and you are Selling BTC for ZAR, your fee will be charged in ZAR
        """
        trade_updates = []

        if order.exchange_order_id is not None:
            trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            all_fills_response = await self._api_get(
                path_url=CONSTANTS.TRADE_HISTORY_PATH_URL % trading_pair,
                is_auth_required=True,
                limit_id=CONSTANTS.TRADE_HISTORY_PATH_URL)

            fee_currency = valr_utils.get_fee_currency(order)

            for trade in all_fills_response:
                exchange_order_id = str(trade["orderId"])
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    # TODO: is this needed?
                    percent_token=fee_currency
                )
                trade_update = TradeUpdate(
                    trade_id=str(trade["id"]),
                    client_order_id=order.client_order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=trading_pair,
                    fee=fee,
                    fill_base_amount=Decimal(trade["quantity"]),
                    # TODO what to do here?
                    fill_quote_amount=Decimal(0),
                    fill_price=Decimal(trade["price"]),
                    fill_timestamp=iso_to_epoch_seconds(trade["tradedAt"]),
                )
                trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        updated_order_data = await self._api_get(
            path_url=CONSTANTS.ORDER_STATUS_PATH_URL.format(trading_pair, tracked_order.client_order_id),
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_STATUS_PATH_URL)

        new_state = CONSTANTS.ORDER_STATE[updated_order_data["orderStatusType"]]

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data["orderId"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=iso_to_epoch_seconds(updated_order_data["orderUpdatedAt"]) * 1e-3,
            new_state=new_state,
        )

        return order_update

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return build_api_factory(self._throttler, self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return ValrOrderBookTrackerDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return ValrUserStreamTrackerDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List[Dict[str, Any]]):
        mapping = bidict()
        for symbol_data in filter(valr_utils.is_exchange_information_valid, exchange_info):
            mapping[symbol_data["symbol"]] = combine_to_hb_trading_pair(base=symbol_data["baseCurrency"],
                                                                        quote=symbol_data["quoteCurrency"])
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        response = await self._api_get(
            path_url=CONSTANTS.MARKET_SUMMARY_PAIR_PATH_URL % symbol,
            limit_id=CONSTANTS.MARKET_SUMMARY_PAIR_PATH_URL,
        )

        return float(response["lastTradedPrice"])
