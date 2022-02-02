import time
from collections import deque
from datetime import datetime

import backtrader as bt
from backtrader.feed import DataBase
from backtrader.utils.py3 import with_metaclass

from .cryptostore import CryptoStore


class MetaCryptoFeed(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaCryptoFeed, cls).__init__(name, bases, dct)

        # Register with the store
        CryptoStore.DataCls = cls


class CryptoFeed(with_metaclass(MetaCryptoFeed, DataBase)):
    """
    CryptoCurrency eXchange Trading Library Data Feed.
    Params:
      - ``historical`` (default: ``False``)
        If set to ``True`` the data feed will stop after doing the first
        download of data.
        The standard data feed parameters ``fromdate`` and ``todate`` will be
        used as reference.
      - ``backfill_start`` (default: ``True``)
        Perform backfilling at the start. The maximum possible historical data
        will be fetched in a single request.

    Changes From Ed's pacakge

        - Added option to send some additional fetch_ohlcv_params. Some exchanges (e.g Bitmex)
          support sending some additional fetch parameters.
        - Added drop_newest option to avoid loading incomplete candles where exchanges
          do not support sending ohlcv params to prevent returning partial data

    """

    params = (
        ('historical', False),  # only historical download
        ('backfill_start', False),  # do backfilling at the start
        ('fetch_ohlcv_params', {}),
        ('ohlcv_limit', 20),
        ('drop_newest', False),
        ('debug', False)
    )

    _store = CryptoStore

    # States for the Finite State Machine in _load
    _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(3)

    # def __init__(self, exchange, symbol, ohlcv_limit=None, config={}, retries=5):
    def __init__(self, **kwargs):
        # self.store = CryptoStore(exchange, config, retries)
        self.store = self._store(**kwargs)
        self._data = deque()  # data queue for price data
        self._last_id = ''  # last processed trade id for ohlcv
        self._last_ts = 0  # last processed timestamp for ohlcv
        self._ts_delta = None  # timestamp delta for ohlcv

    def start(self, ):
        DataBase.start(self)

        if self.p.fromdate:
            self._state = self._ST_HISTORBACK
            self.put_notification(self.DELAYED)
            self._fetch_ohlcv(self.p.fromdate)

        else:
            self._state = self._ST_LIVE
            self.put_notification(self.LIVE)

    def _load(self):
        if self._state == self._ST_OVER:
            return False

        while True:
            if self._state == self._ST_LIVE:
                if self._timeframe == bt.TimeFrame.Ticks:
                    return self._load_ticks()
                else:
                    # INFO: Fix to address slow loading time after enter into LIVE state.
                    if len(self._data) == 0:
                        # INFO: Only call _fetch_ohlcv when self._data is fully consumed as it will cause execution
                        #       inefficiency due to network latency. Furthermore it is extremely inefficiency to fetch
                        #       an amount of bars but only load one bar at a given time.
                        self._fetch_ohlcv()
                    ret = self._load_ohlcv()
                    if self.p.debug:
                        print('----     LOAD    ----')
                        print('{} Load OHLCV Returning: {}'.format(datetime.utcnow(), ret))
                    return ret

            elif self._state == self._ST_HISTORBACK:
                ret = self._load_ohlcv()
                if ret:
                    return ret
                else:
                    # End of historical data
                    if self.p.historical:  # only historical
                        self.put_notification(self.DISCONNECTED)
                        self._state = self._ST_OVER
                        return False  # end of historical
                    else:
                        self._state = self._ST_LIVE
                        self.put_notification(self.LIVE)

    def _fetch_ohlcv(self, fromdate=None):
        """Fetch OHLCV data into self._data queue"""
        granularity = self.store.get_granularity(self._timeframe, self._compression)
        if self.store.cache is not None and self.p.todate:
            print("Loading from cache", self.p.dataname, granularity, fromdate, self.p.todate)
            data = sorted(self.store.cache.query(self.p.dataname, granularity, fromdate, self.p.todate))
            if self.p.drop_newest:
                del data[-1]
            if len(data) > 0:
                self._data.extend(data)
                self._last_ts = data[-1][0]
        else:
            till = int((self.p.todate - datetime(1970, 1, 1)).total_seconds() * 1000) if self.p.todate else None

            if fromdate:
                since = int((fromdate - datetime(1970, 1, 1)).total_seconds() * 1000)
            else:
                if self._last_ts > 0:
                    if self._ts_delta is None:
                        since = self._last_ts
                    else:
                        since = self._last_ts + self._ts_delta
                else:
                    since = None

            limit = self.p.ohlcv_limit

            while True:
                dlen = len(self._data)

                if self.p.debug:
                    # TESTING
                    since_dt = datetime.utcfromtimestamp(since // 1000) if since is not None else 'NA'
                    print('---- NEW REQUEST ----')
                    print('{} - Requesting: Since TS:{} Since date:{} granularity:{}, limit:{}, params:{}'.format(
                        datetime.utcnow(), since, since_dt, granularity, limit, self.p.fetch_ohlcv_params))
                    data = sorted(self.store.fetch_ohlcv(self.p.dataname, timeframe=granularity,
                                                         since=since, limit=limit, params=self.p.fetch_ohlcv_params))
                    try:
                        for i, ohlcv in enumerate(data):
                            tstamp, open_, high, low, close, volume = ohlcv
                            print('{} - Data {}: {} - TS {} Time {}'.format(datetime.utcnow(), i,
                                                                            datetime.utcfromtimestamp(tstamp // 1000),
                                                                            tstamp, (time.time() * 1000)))
                            # ------------------------------------------------------------------
                    except IndexError:
                        print('Index Error: Data = {}'.format(data))
                    print('---- REQUEST END ----')
                else:

                    data = sorted(self.store.fetch_ohlcv(self.p.dataname, timeframe=granularity,
                                                         since=since, limit=limit, params=self.p.fetch_ohlcv_params))

                # Check to see if dropping the latest candle will help with
                # exchanges which return partial data
                if self.p.drop_newest:
                    del data[-1]

                prev_tstamp = None
                tstamp = None
                for ohlcv in data:
                    if None in ohlcv:
                        continue

                    tstamp = ohlcv[0]

                    if prev_tstamp is not None and self._ts_delta is None:
                        # INFO: Record down the TS delta so that it can be used to increment since TS
                        self._ts_delta = tstamp - prev_tstamp

                    # Prevent from loading incomplete data
                    # if tstamp > (time.time() * 1000):
                    #    continue

                    if tstamp > self._last_ts:
                        if self.p.debug:
                            print('Adding: {}'.format(ohlcv))
                        self._data.append(ohlcv)
                        self._last_ts = tstamp

                    if till and tstamp >= till:
                        break

                    if prev_tstamp is None:
                        prev_tstamp = tstamp


                # print("?", tstamp, till, dlen, len(self._data))

                if till and tstamp is not None:
                    if tstamp >= till:
                        break
                    since = tstamp

                if dlen == len(self._data):
                    break

    def _load_ticks(self):
        if self._last_id is None:
            # first time get the latest trade only
            trades = [self.store.fetch_trades(self.p.dataname)[-1]]
        else:
            trades = self.store.fetch_trades(self.p.dataname)

        if len(trades) <= 1:
            if len(trades) == 1:
                trade = trades[0]
                trade_time = datetime.strptime(trade['datetime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                self._data.append((trade_time, float(trade['price']), float(trade['amount'])))
        else:
            trade_dict_list = []
            index = 0
            # Since we only need the last 2 trades, just loop through the last 2 trades to speed up the for loop
            for trade in trades[-2:]:
                trade_id = trade['id']

                if trade_id > self._last_id:
                    trade_time = datetime.strptime(trade['datetime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    trade_dict = dict(index=index, trade_time=trade_time, price=float(trade['price']),
                                      amount=float(trade['amount']))
                    trade_dict_list.append(trade_dict)
                    self._last_id = trade_id
                    index += 1

            if len(trade_dict_list) > 0:
                # The order of self._data should be in reversed order by trade datetime
                reverse = True
                selection_key = 'index'
                trade_dict_list.sort(key = lambda k: k[selection_key], reverse = reverse)   # sorts in place
                for trade_dict in trade_dict_list:
                    self._data.append((trade_dict['trade_time'], trade_dict['price'], trade_dict['amount']))
                    # Break here once we got the first data is sufficient as we only look for the first data
                    break

            try:
                trade = self._data.popleft()
            except IndexError:
                return None  # no data in the queue

            trade_time, price, size = trade

            self.lines.datetime[0] = bt.date2num(trade_time)
            self.lines.open[0] = price
            self.lines.high[0] = price
            self.lines.low[0] = price
            self.lines.close[0] = price
            self.lines.volume[0] = size

        return True

    def _load_ohlcv(self):
        try:
            ohlcv = self._data.popleft()
        except IndexError:
            return None  # no data in the queue

        tstamp, open_, high, low, close, volume = ohlcv

        dtime = datetime.utcfromtimestamp(tstamp // 1000)

        self.lines.datetime[0] = bt.date2num(dtime)
        self.lines.open[0] = open_
        self.lines.high[0] = high
        self.lines.low[0] = low
        self.lines.close[0] = close
        self.lines.volume[0] = volume

        return True

    def haslivedata(self):
        return self._state == self._ST_LIVE and self._data

    def islive(self):
        return not self.p.historical
