from decimal import Decimal
from typing import (
    Any,
    Dict,
    Optional,
)
import asyncio
from hummingbot.core.event.events import (
    OrderType,
    TradeType
)
from hummingbot.connector.in_flight_order_base import InFlightOrderBase


class ValrInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: Optional[str],
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 initial_state: str = "PLACED"):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state,
        )
        self.trade_id_set = set()
        self.cancelled_event = asyncio.Event()

    @property
    def is_done(self) -> bool:
        return self.last_state in ["FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "FAILED"]

    @property
    def is_failure(self) -> bool:
        return self.last_state in ["REJECTED", "FAILED"]

    @property
    def is_cancelled(self) -> bool:
        return self.last_state in ["CANCELED", "CANCELLED", "EXPIRED"]

    # @property
    # def order_type_description(self) -> str:
    #     """
    #     :return: Order description string . One of ["limit buy" / "limit sell" / "market buy" / "market sell"]
    #     """
    #     order_type = "market" if self.order_type is OrderType.MARKET else "limit"
    #     side = "buy" if self.trade_type == TradeType.BUY else "sell"
    #     return f"{order_type} {side}"

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> InFlightOrderBase:
        """
        :param data: json data from API
        :return: formatted InFlightOrder
        """
        retval = ValrInFlightOrder(
            data["client_order_id"],
            data["exchange_order_id"],
            data["trading_pair"],
            getattr(OrderType, data["order_type"]),
            getattr(TradeType, data["trade_type"]),
            Decimal(data["price"]),
            Decimal(data["amount"]),
            data["last_state"]
        )
        retval.executed_amount_base = Decimal(data["executed_amount_base"])
        retval.executed_amount_quote = Decimal(data["executed_amount_quote"])
        retval.fee_asset = data["fee_asset"]
        retval.fee_paid = Decimal(data["fee_paid"])
        retval.last_state = data["last_state"]
        return retval

    def update_with_trade_update(self, trade_update: Dict[str, Any]) -> bool:
        """
        Updates the in flight order with trade update (from private/get-order-detail end point)
        return: True if the order gets updated otherwise False
        """

        # {
        #   "price": "9500",
        #   "quantity": "0.00105263",
        #   "currencyPair": "BTCZAR",
        #   "tradedAt": "2019-04-25T20:36:53.426Z",
        #   "side": "buy",
        #   "orderId":"d5a81b99-fabf-4be1-bc7c-1a00d476089d",
        #   "id":"7a2b5560-5a71-4640-9e4b-d659ed26278a"
        # }
        trade_id = trade_update["id"]
        # trade_update["orderId"] is type int
        if str(trade_update["orderId"]) != self.exchange_order_id or trade_id in self.trade_id_set:
            # trade already recorded
            return False
        self.trade_id_set.add(trade_id)
        self.executed_amount_base += Decimal(str(trade_update["quantity"]))
        # self.fee_paid += Decimal(str(trade_update["fee"]))
        self.fee_paid += Decimal(0)
        self.executed_amount_quote += (Decimal(str(trade_update["price"])) *
                                       Decimal(str(trade_update["quantity"])))
        return True
