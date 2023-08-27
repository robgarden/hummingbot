from decimal import Decimal
from typing import Any, Dict

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTCZAR"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("-0.0001"),
    taker_percent_fee_decimal=Decimal("0.001"),
    buy_percent_fee_deducted_from_returns=True
)


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate with based on its exchange information
    :param exchange_info: the exchange information for a trading pair
    :return: True if the trading pair is enabled, False otherwise
    """
    return exchange_info.get("active", None) is True and exchange_info.get("currencyPairType", None) == "SPOT"


def get_fee_currency(order: InFlightOrder):
    """
    Fee currency:
        If you are a Maker and you are Buying BTC with ZAR, your reward will be paid in ZAR
        If you are a Maker and you are Selling BTC for ZAR, your reward will be paid in BTC
        If you are a Taker and you are Buying BTC with ZAR, your fee will be charged in BTC
        If you are a Taker and you are Selling BTC for ZAR, your fee will be charged in ZAR
    """
    base, quote = order.trading_pair.split("-")
    fee_currency_map = {
        OrderType.LIMIT_MAKER: {
            TradeType.BUY: quote,
            TradeType.SELL: base,
        },
        OrderType.LIMIT: {
            TradeType.BUY: base,
            TradeType.SELL: quote,
        },
        OrderType.MARKET: {
            TradeType.BUY: base,
            TradeType.SELL: quote,
        }
    }
    return fee_currency_map[order.order_type][order.trade_type]


class ValrConfigMap(BaseConnectorConfigMap):
    connector: str = Field(default="valr", const=True, client_data=None)
    valr_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your VALR API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )
    valr_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your VALR API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "valr"


KEYS = ValrConfigMap.construct()
