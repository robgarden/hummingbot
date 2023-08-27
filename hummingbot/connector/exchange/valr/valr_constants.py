from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

HBOT_ORDER_ID_PREFIX = "x-HBOT-"
MAX_ORDER_ID_LEN = 32

# Base URL
BASE_URL = "api.valr.com"
# BASE_URL = "api-dev.valr.world"
REST_URL = "https://%s" % BASE_URL
WSS_URL = "wss://%s" % BASE_URL

# Public API endpoints
MARKET_SUMMARY_PATH_URL = "/v1/public/marketsummary"
MARKET_SUMMARY_PAIR_PATH_URL = "/v1/public/%s/marketsummary"
PAIRS_PATH_URL = "/v1/public/pairs"
STATUS_PATH_URL = "/v1/public/status"
SERVER_TIME_PATH_URL = "/v1/public/time"

# Private API endpoints or BinanceClient function
BALANCES_PATH_URL = "/v1/account/balances?excludeZeroBalances=true"
ORDERS_OPEN_PATH_URL = "/v1/orders/open"
ORDERS_CANCEL_PATH_URL = "/v1/orders/order"
ORDERS_LIMIT_PATH_URL = "/v1/orders/limit"
ORDERS_MARKET_PATH_URL = "/v1/orders/market"
ORDERS_BATCH_PATH_URL = "/v1/batch/orders"
ORDER_STATUS_PATH_URL = "/v1/orders/{}/customerorderid/{}"
ORDER_BOOK_FULL_PATH_URL = "/v1/marketdata/%s/orderbook/full"
TRADE_HISTORY_PATH_URL = "/v1/account/%s/tradehistory"

# Websocket endpoints
WSS_ACCOUNT_PATH = "/ws/account"
WSS_ACCOUNT_URL = WSS_URL + WSS_ACCOUNT_PATH
WSS_TRADE_PATH = "/ws/trade"
WSS_TRADE_URL = WSS_URL + WSS_TRADE_PATH

WS_HEARTBEAT_TIME_INTERVAL = 30

SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

TIME_IN_FORCE_GTC = "GTC"  # Good till cancelled
TIME_IN_FORCE_IOC = "IOC"  # Immediate or cancel
TIME_IN_FORCE_FOK = "FOK"  # Fill or kill

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1

# Order States
ORDER_STATE = {
    "Pending": OrderState.PENDING_CREATE,
    "New": OrderState.OPEN,
    "Placed": OrderState.OPEN,
    "Filled": OrderState.FILLED,
    "Partially Filled": OrderState.PARTIALLY_FILLED,
    "Pending Cancel": OrderState.OPEN,
    "Cancelled": OrderState.CANCELED,
    "Rejected": OrderState.FAILED,
    "Expired": OrderState.FAILED,
}

# Websocket event types
FULL_ORDERBOOK_SNAPSHOT_EVENT_TYPE = "FULL_ORDERBOOK_SNAPSHOT"
FULL_ORDERBOOK_UPDATE_EVENT_TYPE = "FULL_ORDERBOOK_UPDATE"
NEW_TRADE_EVENT_TYPE = "NEW_TRADE"
BALANCE_UPDATE_EVENT_TYPE = 'BALANCE_UPDATE'
ORDER_STATUS_UPDATE_EVENT_TYPE = 'ORDER_STATUS_UPDATE'

# Rate limits
ALL_HTTP_LIMIT_ID = "ALL_HTTP_LIMIT_ID"
ALL_HTTP_LIMIT = 1000
ALL_HTTP_LIMIT_INTERVAL = ONE_MINUTE

ALL_WS_LIMIT_ID = "ALL_WS_LIMIT_ID"
ALL_WS_LIMIT = 30
ALL_WS_LIMIT_INTERVAL = ONE_MINUTE

HIGH_RPS_LIMIT_ID = "HIGH_RPS_LIMIT_ID"
HIGH_RPS_LIMIT = 230
HIGH_RPS_LIMIT_INTERVAL = ONE_SECOND

BATCH_ORDERS_LIMIT_ID = "BATCH_ORDERS_LIMIT_ID"
BATCH_ORDERS_LIMIT = 230
BATCH_ORDERS_LIMIT_INTERVAL = ONE_SECOND

DELETE_ORDERS_LIMIT_ID = "DELETE_ORDERS_LIMIT_ID"
DELETE_ORDERS_LIMIT = 230
DELETE_ORDERS_LIMIT_INTERVAL = ONE_SECOND

POST_ORDERS_LIMIT_ID = "POST_ORDERS_LIMIT_ID"
POST_ORDERS_LIMIT = 230
POST_ORDERS_LIMIT_INTERVAL = ONE_SECOND

RATE_LIMITS = [
    # Pools
    # Http limits (applies to most endpoints except high rps endpoint such as orders and marketdata)
    RateLimit(limit_id=ALL_HTTP_LIMIT_ID, limit=ALL_HTTP_LIMIT, time_interval=ALL_HTTP_LIMIT_INTERVAL),
    # Ws limits
    RateLimit(limit_id=ALL_WS_LIMIT_ID, limit=ALL_WS_LIMIT, time_interval=ALL_WS_LIMIT_INTERVAL),
    # High RPS limits
    RateLimit(limit_id=HIGH_RPS_LIMIT_ID, limit=HIGH_RPS_LIMIT, time_interval=HIGH_RPS_LIMIT_INTERVAL),

    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=ALL_HTTP_LIMIT, time_interval=ALL_HTTP_LIMIT_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_HTTP_LIMIT_ID)]),
    RateLimit(limit_id=PAIRS_PATH_URL, limit=ALL_HTTP_LIMIT, time_interval=ALL_HTTP_LIMIT_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_HTTP_LIMIT_ID)]),
    RateLimit(limit_id=MARKET_SUMMARY_PAIR_PATH_URL, limit=ALL_HTTP_LIMIT, time_interval=ALL_HTTP_LIMIT_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_HTTP_LIMIT_ID)]),
    RateLimit(limit_id=STATUS_PATH_URL, limit=ALL_HTTP_LIMIT, time_interval=ALL_HTTP_LIMIT_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_HTTP_LIMIT_ID)]),
    RateLimit(limit_id=BALANCES_PATH_URL, limit=ALL_HTTP_LIMIT, time_interval=ALL_HTTP_LIMIT_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_HTTP_LIMIT_ID)]),
    RateLimit(limit_id=TRADE_HISTORY_PATH_URL, limit=ALL_HTTP_LIMIT, time_interval=ALL_HTTP_LIMIT_INTERVAL,
              linked_limits=[LinkedLimitWeightPair(ALL_HTTP_LIMIT_ID)]),

    RateLimit(limit_id=ORDERS_CANCEL_PATH_URL, limit=230, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(HIGH_RPS_LIMIT_ID)]),
    RateLimit(limit_id=ORDERS_LIMIT_PATH_URL, limit=230, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(HIGH_RPS_LIMIT_ID)]),
    RateLimit(limit_id=ORDERS_MARKET_PATH_URL, limit=230, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(HIGH_RPS_LIMIT_ID)]),
    RateLimit(limit_id=ORDERS_BATCH_PATH_URL, limit=230, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(HIGH_RPS_LIMIT_ID)]),
    RateLimit(limit_id=ORDER_BOOK_FULL_PATH_URL, limit=230, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(HIGH_RPS_LIMIT_ID)]),
    RateLimit(limit_id=ORDER_STATUS_PATH_URL, limit=230, time_interval=ONE_SECOND,
              linked_limits=[LinkedLimitWeightPair(HIGH_RPS_LIMIT_ID)]),
]

# ORDER_NOT_EXIST_ERROR_CODE = -2013
# ORDER_NOT_EXIST_MESSAGE = "Order does not exist"
# UNKNOWN_ORDER_ERROR_CODE = -2011
# UNKNOWN_ORDER_MESSAGE = "Unknown order sent"
