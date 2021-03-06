from ib_insync import IB, util

from logger import logger
from trader import Candle, Trader, Blotter, get_contracts


log = logger(__file__[:-3])

ib = IB()
ib.connect('127.0.0.1', 4002, clientId=10)


contracts = [
    ('NQ', 'GLOBEX'),
    ('ES', 'GLOBEX'),
    ('NKD', 'GLOBEX'),
    ('CL', 'NYMEX'),
    ('GC', 'NYMEX'),
]

# util.patchAsyncio()
blotter = Blotter()
trader = Trader(ib, blotter)
futures = get_contracts(contracts, ib)
candles = [Candle(contract, trader, ib)
           for contract in futures]


ib.run()
