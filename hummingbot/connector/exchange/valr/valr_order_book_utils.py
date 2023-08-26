import zlib
from typing import Any, Dict, List


def keep_last_occurrence(orders):
    result = []
    seen = {}

    for order in orders:
        id = order["orderId"]
        if id not in seen:
            latest_order = [o for o in orders if o["orderId"] == id][-1]
            seen[id] = True
            if latest_order["quantity"] != "0":
                result.append(latest_order)

    return result


def apply_diff(order_book: Dict[str, Any], diff_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    TODO: optimize
    """
    def calculate_new_side(side: str, book: Dict[str, Any]) -> List[Any]:
        new_side = []
        diff_side = diff_data[side]

        all_price_levels = sorted(
            list(set([level["Price"] for level in book[side]] + [level["Price"] for level in diff_side])),
            key=lambda x: x,
            reverse=side == "Bids"
        )

        for price in all_price_levels:
            diff_orders = ([level for level in diff_side if level["Price"] == price] or [{"Orders": []}])[0].get(
                "Orders")
            book_orders = ([level for level in book[side] if level["Price"] == price] or [{"Orders": []}])[0].get(
                "Orders")
            all_orders = book_orders + diff_orders

            final_orders = keep_last_occurrence(all_orders)
            if final_orders:
                new_side.append({"Price": price, "Orders": final_orders})

        return new_side

    new_book = {
        "Bids": calculate_new_side("Bids", order_book),
        "Asks": calculate_new_side("Asks", order_book),
    }
    return new_book


def calculate_checksum(book: Dict[str, Any]) -> int:
    def select_best_orders(side: str) -> List[any]:
        reverse = side == "Bids"
        price_levels_sorted = sorted(book[side], key=lambda s: float(s["Price"]), reverse=reverse)
        retval = []
        for price_level in price_levels_sorted:
            for order in price_level["Orders"]:
                if len(retval) == 25:
                    return retval
                if order["quantity"] == "0":
                    raise AssertionError("Order ID: %s" % order["orderId"])
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
