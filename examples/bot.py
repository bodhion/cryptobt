from cryptobt import CryptoStore
import backtrader as bt
import json
import os
from datetime import datetime
from strategy import TestStrategy


def start(config, name, strategy, start, end=None, timeframe=bt.TimeFrame.Minutes, compression=1, debug=False):
    exchange = config.get("exchange")
    currency = config.get("currency")
    broker_mapping = config.get("broker_mapping")
    exchange_config = config.get("exchange_config")
    sandbox = config.get("sandbox")
    retries = config.get("retries")
    cash = config.get("cash")
    commission = config.get("commission")
    ohlcv_limit = config.get("ohlcv_limit")

    cerebro = bt.Cerebro()
    cerebro.addstrategy(strategy)

    live = end is None
    store = CryptoStore(exchange=exchange, currency=currency, config=exchange_config,
                        retries=retries, sandbox=sandbox, debug=debug)

    if live:
        broker = store.getbroker(broker_mapping=broker_mapping)
        cerebro.setbroker(broker)
        data = store.getdata(dataname=name, name=name, timeframe=timeframe, fromdate=start,
                             compression=compression, ohlcv_limit=ohlcv_limit, drop_newest=True, historical=False)
    else:
        cerebro.broker.setcash(cash)
        cerebro.broker.set_shortcash(True)
        cerebro.broker.setcommission(commission=commission)
        data = store.getdata(dataname=name, name=name, timeframe=timeframe, fromdate=start,
                             todate=end, compression=compression, ohlcv_limit=ohlcv_limit, drop_newest=False,
                             historical=True, debug=debug)

    cerebro.adddata(data)
    cerebro.run()
    if not live:
        cerebro.plot()


if __name__ == "__main__":
    if not os.path.isfile("config.json"):
        print("Please copy the config_example.json to config.json and update API keys within the file.")
        exit(-1)

    with open("config.json", "r") as f:
        config = json.load(f)
    start(config, "BTC-PERPETUAL",
          strategy=TestStrategy, start=datetime(2022, 1, 1), end=datetime(2022, 2, 1),
          timeframe=bt.TimeFrame.Minutes, compression=1, debug=True)
