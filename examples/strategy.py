import backtrader as bt
from datetime import datetime, timedelta


class NaiveLongShortStrategy(bt.Strategy):
    def __init__(self, ratio=0.9):
        self.dataclose = self.datas[0].close
        self.ratio = ratio
        # To keep track of pending orders and buy price/commission
        self.order = None


    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            print(self.datas[0].datetime.datetime(), "buy" if order.isbuy() else "sell", order.executed.size,
                  order.executed.price, order.executed.comm)

        self.order = None

    def next(self):
        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Amount is quantity of the instrument equal to current portfolio value times quantity factor
        amount = (self.broker.getvalue() * self.ratio)
        # amount = (self.broker.getvalue() * self.ratio) / self.dataclose[0]

        pos = abs(self.position.size)
        buy_sig = self.buy_signal()
        sell_sig = self.sell_signal()

        if buy_sig:
            if self.position.size < 0:  # already short, need to buy back the short position
                amount = pos + amount

            if self.position.size <= 0:  # Shouldn't buy when long
                self.order = self.buy(size=amount)
        elif sell_sig:
            if self.position.size > 0:  # already long, need to sell the current long position
                amount = pos + amount

            if self.position.size >= 0:  # Shouldn't sell when short (IMPORTANT)
                self.order = self.sell(size=amount)


class TestStrategy(NaiveLongShortStrategy):
    def __init__(self):
        super(TestStrategy, self).__init__()

        self.ma = bt.indicators.SimpleMovingAverage(self.datas[0], period=21)
        self.cross = bt.ind.CrossOver(self.dataclose, self.ma)

    def buy_signal(self):
        return self.cross > 0

    def sell_signal(self):
        return self.cross < 0
