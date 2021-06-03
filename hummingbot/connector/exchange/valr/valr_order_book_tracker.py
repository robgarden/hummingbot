#!/usr/bin/env python
import asyncio
import logging

from collections import defaultdict, deque
from typing import Optional, Dict, List, Deque
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker

from . import valr_constants as constants
from .valr_order_book_message import ValrOrderBookMessage
from .valr_active_order_tracker import ValrActiveOrderTracker
from .valr_api_order_book_data_source import ValrAPIOrderBookDataSource
from .valr_order_book import ValrOrderBook


class ValrOrderBookTracker(OrderBookTracker):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pairs: Optional[List[str]] = None,):
        super().__init__(ValrAPIOrderBookDataSource(trading_pairs), trading_pairs)

        self._ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        self._order_book_snapshot_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_diff_stream: asyncio.Queue = asyncio.Queue()
        self._order_book_trade_stream: asyncio.Queue = asyncio.Queue()
        self._process_msg_deque_task: Optional[asyncio.Task] = None
        self._past_diffs_windows: Dict[str, Deque] = {}
        self._order_books: Dict[str, ValrOrderBook] = {}
        self._saved_message_queues: Dict[str, Deque[ValrOrderBookMessage]] = \
            defaultdict(lambda: deque(maxlen=1000))
        self._active_order_trackers: Dict[str, ValrActiveOrderTracker] = defaultdict(ValrActiveOrderTracker)
        self._order_book_stream_listener_task: Optional[asyncio.Task] = None
        self._order_book_trade_listener_task: Optional[asyncio.Task] = None

    @property
    def exchange_name(self) -> str:
        """
        Name of the current exchange
        """
        return constants.EXCHANGE_NAME
