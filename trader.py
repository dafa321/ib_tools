import sys
from collections import defaultdict
from datetime import datetime

import pandas as pd

from eventkit import Event
from ib_insync import IB, util, MarketOrder, StopOrder
from ib_insync.contract import ContFuture, Future

from logbook import Logger
from indicators import get_ATR, get_signals


log = Logger(__name__)


class BarStreamer:

    def __init__(self):
        self.bars = self.get_bars(self.contract)
        self.new_bars = []

    def get_bars(self, contract):
        return self.ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='4 D',
            barSizeSetting='30 secs',
            whatToShow='TRADES',
            useRTH=False,
            formatDate=1,
            keepUpToDate=True)

    def subscribe(self):
        def onNewBar(bars, hasNewBar):
            if hasNewBar:
                self.aggregate(bars[-2])
        self.bars.updateEvent += onNewBar

    def process_back_data(self):
        self.backfill = True
        for bar in self.bars[:-2]:
            self.aggregate(bar)
        self.backfill = False

    def aggregate(self, bar):
        raise NotImplementedError


class VolumeCandle(BarStreamer):

    def __init__(self, avg_periods=30, span=5500):
        super().__init__()
        self.span = span
        self.avg_periods = avg_periods
        self.aggregator = 0
        self.reset_volume()
        self.process_back_data()
        self.subscribe()

    def reset_volume(self):
        """
        self.volume = util.df(self.bars).volume \
            .rolling(self.avg_periods).sum() \
            .ewm(span=self.span).mean().iloc[-1].round()
        """

        self.volume = util.df(self.bars).volume \
            .rolling(self.avg_periods).sum() \
            .mean().round()

    def aggregate(self, bar):
        self.new_bars.append(bar)
        self.aggregator += bar.volume
        if not self.backfill:
            message = (f'{bar.date} {self.aggregator}/{self.volume}'
                       f' {self.contract.localSymbol}')
            log.debug(message)
        if self.aggregator >= self.volume:
            self.aggregator = 0
            #self.aggregator -= self.volume
            self.create_candle()

    def create_candle(self):
        df = util.df(self.new_bars)
        self.new_bars = []
        df.date = df.date.astype('datetime64')
        df.set_index('date', inplace=True)
        df['backfill'] = True
        df['volume_weighted'] = (df.close + df.open)/2 * df.volume
        weighted_price = df.volume_weighted.sum() / df.volume.sum()
        if not self.backfill:
            log.debug(f'new candle created for {self.contract.localSymbol}')
        self.append({'backfill': self.backfill,
                     'date': df.index[-1],
                     'open': df.open[0],
                     'high': df.high.max(),
                     'low': df.low.min(),
                     'close': df.close[-1],
                     'price': weighted_price,
                     'volume': df.volume.sum()})


class SignalProcessor:

    def __init__(self, ib):
        self.ib = ib
        self._createEvents()

    def positions(self):
        positions = self.ib.positions()
        return {p.contract: p.position for p in positions}

    def _createEvents(self):
        self.entrySignal = Event('entrySignal')
        self.closeSignal = Event('closeSignal')

    def entry(self, contract, signal, atr):
        positions = self.positions()
        if not positions.get(contract):
            message = (f'entry signal emitted for {contract.localSymbol},'
                       f'signal: {signal}, atr: {atr}')
            log.debug(message)
            self.entrySignal.emit(contract, signal, atr)
        else:
            self.breakout(contract, signal)

    def breakout(self, contract, signal):
        positions = self.positions()
        if positions.get(contract):
            if positions.get(contract) * signal < 0:
                log.debug('close signal emitted')
                self.closeSignal.emit(contract, signal)


class Candle(VolumeCandle):

    periods = [5, 10, 20, 40, 80, ]  # 160, ]
    ema_fast = 120  # number of periods for moving average filter
    sl_atr = 1  # stop loss in ATRs
    atr_periods = 80  # number of periods to calculate ATR on
    time_int = 30  # interval in minutes to be used to define volume candle

    def __init__(self, contract, trader, ib):
        self.contract = contract
        self.ib = ib
        log.debug(f'candle init for contract {self.contract.localSymbol}')
        self.candles = []
        self.counter = 0
        self.processor = SignalProcessor(ib)
        #self.processor.entrySignal.connect(trader.onEntry, keep_ref=True)
        #self.processor.closeSignal.connect(trader.onClose, keep_ref=True)
        self.processor.entrySignal += trader.onEntry
        self.processor.closeSignal += trader.onClose
        self.processor.entrySignal += print
        self.processor.closeSignal += print
        log.debug(self.processor.entrySignal._slots)
        log.debug(self.processor.closeSignal._slots)

        super().__init__()

    def freeze(self):
        if self.counter == 0:
            self.df.to_pickle(
                f'notebooks/freeze/freeze_df_{self.contract.localSymbol}.pickle')
            log.debug(f'freezed data saved for {self.contract.localSymbol}')
            self.counter += 1

    def append(self, candle):
        self.candles.append(candle)
        self.get_indicators()

    def get_indicators(self):
        self.df = pd.DataFrame(self.candles)
        self.df.set_index('date', inplace=True)
        self.df['ema_fast'] = self.df.price.ewm(span=self.ema_fast).mean()
        self.df['atr'] = get_ATR(self.df, self.atr_periods)
        self.df['signal'] = get_signals(self.df.price, self.periods)
        if not self.backfill:
            positions = [(p.contract.localSymbol, p.position)
                         for p in self.ib.positions()]
            log.info(f'POSITIONS: {positions}')
            log.debug(
                f'{self.contract.localSymbol} {self.df.iloc[-1].to_dict()}')

        self.process()

    def process(self):
        if self.df.backfill[-1]:
            return
        else:
            self.freeze()
        if self.df.signal[-1]:
            if self.df.signal[-1] * (self.df.price[-1] - self.df.ema_fast[-1]) > 0:
                self.processor.entry(
                    self.contract, self.df.signal[-1], self.df.atr[-1])
            elif self.df.signal[-1]:
                self.processor.breakout(self.contract, self.df.signal[-1])


class Trader:

    def __init__(self, ib):
        log.debug('Trader initialized')
        self.ib = ib
        self.atr_dict = {}

    def onEntry(self, contract, signal, atr):
        log.debug(f'entry signal handled for: {contract} {signal} {atr}')
        self.atr_dict[contract] = atr
        trade = self.trade(contract, signal)
        trade.filledEvent += self.attach_sl
        log.debug('entry order placed')

    def onClose(self, contract, signal):
        log.debug(f'close signal handled for: {contract} {signal}')
        positions = {p.contract: p.position for p in self.ib.positions()}
        if contract in positions:
            trade = self.trade(contract, signal)
            trade.filledEvent += self.remove_sl
            log.debug('closing order placed')

    def trade(self, contract, signal):
        if signal == 1:
            log.debug(f'entering buy order for {contract.localSymbol}')
            order = MarketOrder('BUY', 1)
        elif signal == -1:
            log.debug('entering sell order for {contract.localSymbol')
            order = MarketOrder('Sell', 1)
        trade = self.ib.placeOrder(contract, order)
        return trade

    def attach_sl(self, trade):
        side = {'BUY': 'SELL', 'SELL': 'BUY'}
        direction = {'BUY': -1, 'SELL': 1}
        contract = trade.contract
        action = trade.order.action
        amount = trade.orderStatus.filled
        price = trade.orderStatus.avgFillPrice
        log.info(f'TRADE EXECUTED: {contract.localSymbol} {action} @{price}')
        sl_points = self.atr_dict[contract]
        # TODO round to the nearest tick
        sl_price = round(price + sl_points * direction[action])
        log.info(f'STOP LOSS PRICE: {sl_price}')
        order = StopOrder(side[action], amount, sl_price,
                          outsideRth=True, tif='GTC')
        trade = self.ib.placeOrder(contract, order)
        trade += self.report_stopout
        log.debug('{contract.localSymbol} stop loss attached')

    def remove_sl(self, trade):
        contract = trade.contract
        open_trades = self.ib.openTrades()
        # open_orders = self.ib.reqAllOpenOrders()
        orders = defaultdict(list)
        for t in open_trades:
            orders[t.contract].append(t.order)
        for order in orders[contract]:
            if order.orderType == 'STP':
                self.ib.cancelOrder(order)
                log.debug('stop loss removed')

    def report_stopout(self, trade):
        message = (
            f'STOP-OUT for {trade.contract}'
            f'{trade.order.action} @{trade.orderStatus.avgFillPrice}'
        )
        log.info(message)
