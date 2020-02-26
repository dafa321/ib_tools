#!/usr/bin/env python3

from datetime import datetime, timedelta
import itertools
import asyncio
import os
import sys
import functools
import logging
#import datetime

import pandas as pd
from ib_insync import IB, util
from ib_insync.contract import Future, ContFuture, Contract,Stock
from logbook import Logger, StreamHandler, FileHandler, set_datetime_format
import myconnutils
from influxdb import InfluxDBClient
from influxdb import SeriesHelper


from connect import IB_connection
from config import max_number_of_workers
from datastore_pytables import Store

#import myconnutils
#from myutils import createLogger,selectSymFromTbl
import random
import configparser
import operator

def make_args():
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("--log", action="store", type=str,required=False,default="myqt.log")
    parser.add_argument("--pt", action="store", type=str,required=False,default="scantbl")
    parser.add_argument("--range", action="store", type=int,required=False,default=4)
    parser.add_argument("--profile", action="store", type=str,required=False,default="/testall.ini")
    parser.add_argument("--testmode", action="store", type=int,required=False,default=1)
    args = parser.parse_args()
    return args


args = make_args()
CONFDIR = os.getenv("QTPY")
config = configparser.ConfigParser()
bConfig = CONFDIR + args.profile 
#logger.info(bConfig)
config.read(bConfig)
tbls=config['TIAnalysis']['scantbls']
logmode=config['scan']['debugMode']
host=config['influx']['host']
port=config['influx']['port']
password=config['influx']['passwd']
dbnameBF=config['influx']['dbnameBF']
seriesBF=config['influx']['seriesBF']
user=config['influx']['user']
durationStr=config['backfill']['durationStr']
barSizeSetting=config['backfill']['barSizeSetting']
backdays=int(config['backfill']['backdays'])

myclient = InfluxDBClient(host, port, user, password, dbnameBF)
myclient.create_database(dbnameBF)

"""
Modelled (loosely) on example here:
https://docs.python.org/3/library/asyncio-queue.html#examples
and here:
https://realpython.com/async-io-python/#using-a-queue
"""

class MySeriesHelper(SeriesHelper):
    """Instantiate SeriesHelper to write points to the backend."""

    class Meta:
        """Meta class stores time series helper configuration."""

        # The client should be an instance of InfluxDBClient.
        client = myclient

        # The series name must be a string. Add dependent fields/tags
        # in curly brackets.
        #series_name = 'series'
        series_name = seriesBF

        # Defines all the fields in this time series.
        #fields = ['last', 'bid','ask','volume','low','high','open']
        #fields = ['open', 'high','low','close','volume','average','barCount']
        fields = ['open', 'high','low','close','volume','average','barCount']
        #fields = ['last', 'bid','ask','volume']

        # Defines all the tags for the series.
        tags = ['BF']

        # Defines the number of data points to store prior to writing
        # on the wire.
        bulk_size = 5

        # autocommit must be set to True when using bulk_size
        autocommit = True

#async def get_history(contract, dt):
async def get_history(contract, dt):
    log.debug(f'loading {contract.symbol} ending {dt} durationStr, barSizeSetting, backdays :{durationStr}, {barSizeSetting}, {backdays}')
    chunk = await ib.reqHistoricalDataAsync(contract,
                                            endDateTime=dt,
                                            durationStr=durationStr,
                                            barSizeSetting=barSizeSetting,
                                            whatToShow='TRADES',
                                            useRTH=True,
                                            formatDate=1,
                                            keepUpToDate=False,
                                            )
    """"
    chunk = await ib.reqHistoricalDataAsync(contract,
                                            endDateTime=dt,
                                            durationStr='10 D',
                                            barSizeSetting='1 min',
                                            whatToShow='TRADES',
                                            useRTH=False,
                                            formatDate=1,
                                            keepUpToDate=False,                                     )
                                            """
    if not chunk:
        log.debug(f'Completed loading for {contract.localSymbol}')
        return None

    #next_chunk = {'dt': chunk[0].date, 'contract': contract}
    next_chunk = {'dt': chunk[len(chuck) -1].date, 'contract': contract}
    log.debug(
        f'downloaded data chunk for: {contract.localSymbol} ending: {dt} 3800 {chunk[3899].date}')
    df = util.df(reversed(chunk))
    df.date = df.date.astype('datetime64')
    df.set_index('date', inplace=True)
    ty=type(df)
    log.debug(df)
    log.debug(type(chunk))
    for i, row in df.iterrows():
        #log.debug(i)
        #log.debug(contract.symbol)
        #beCheck=["After MySeries check",contract.symbol,row['open'],row['high'],row['low'],row['close'],row['volume'],row['average'],row['barCount']]
        #logger.debug(beCheck)
        #MySeriesHelper( dateSTK=contract.symbol,open=row['open'],high=row['high'],low=row['low'],close=row['close'],volume=row['volume'],average=row['average'],barCount=row['barCount'])
        MySeriesHelper(time=i,BF=contract.symbol,open=row['open'],high=row['high'],low=row['low'],close=row['close'],volume=row['volume'],average=row['average'],barCount=row['barCount'])
        #MySeriesHelper( time=row['date'],dateSTK=contract.symbol,open=row['open'],high=row['high'],low=row['low'],close=row['close'],volume=row['volume'],average=row['average'],barCount=row['barCount'])
        #log.debug(row['open'], row['high'], row['low'], row['close'], 
#row['volume'], row['average'], row['barCount']) 
    log.debug(f'before write  data chunk for: {contract.localSymbol} df={df} type={ty}')
    log.debug(df)
    #store.write(contract, df)
        #fields = ['open', 'high','low','close','volume','average','barCount']
    #MySeriesHelper(STK=contract.symbol,bid=t.bid, ask=t.ask,last=t.last,volume=barVolme)
  #  beCheck=["After MySeries check",t.contract.symbol,rangeLow,rangeHiLow, t.ask,  t.low,t.high,t.close,t.volume,barVolme]
  #  logger.debug(beCheck)
    log.debug(f'saved data chunk for: {contract.symbol} next cuheck time{chunk[len(chuck) -1].date}')

    return next_chunk


def lookup_contracts_Stock1(symbols):
    futures = []
    for s in symbols:
        futures.append(
            [Stock(**c.contract.dict())
             for c in ib.reqContractDetails(Stock(**s))])
        #log.debug(futures)
    #return futures
    return futures

def lookup_contracts_Stock3(symbols):
    futures = []
    for s in symbols:
        #log.format_string = '{record.channel}: {record.message}'
        #log.formatter
        log.debug(f'www{s}')
        st=Stock(s['symbol'] ,s['exchange'],s['currency'],primaryExchange=s['primaryExchange'])
        #st= Stock('AMD', 'SMART','NASDAQ','USD') 
        futures.append(st)
    return futures

def lookup_contracts_Stock(symbols):
    futures = []
    for s in symbols:
        #log.format_string = '{record.channel}: {record.message}'
        #log.formatter
        log.debug(f'www{s}')
        #st=Stock(s['symbol'] ,s['exchange'],s['currency'],primaryExchange=s['primaryExchange'])
        st=Stock(s,'SMART','USD',primaryExchange='NASDAQ')
        #st= Stock('AMD', 'SMART','NASDAQ','USD') 
        futures.append(st)
    return futures
def lookup_contracts_Future(symbols):
    futures = []
    for s in symbols:
        futures.append(
            [Future(**c.contract.dict())
             for c in ib.reqContractDetails(Future(**s, includeExpired=True))]
        )
    return list(itertools.chain(*futures))


def lookup_continuous_contracts(symbols):
    return [ContFuture(**s) for s in symbols]

def lookup_contracts(symbols):
    return [Contract(**s) for s in symbols]


async def schedule_task(contract, dt, queue):
    log.debug(f'schedulling task for contract {contract.localSymbol}')
    await queue.put(functools.partial(get_history, contract, dt))


def initial_scheduleBF(contract, days):
    tod = datetime.now()
    d = timedelta(days = days)
    #yyyyMMdd HH:mm:ss
    start = tod - d
    st=start.strftime('%Y%m%d %H:%M:%S')
    return {'contract': contract, 'dt': st}

def initial_schedule(contract, now):
    return {'contract': contract, 'dt':'20191217 15:56:23' }
    earliest = store.check_earliest(contract)
    if earliest:
        dt = earliest
    else:
        dt = min(
            datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d'),
            now)
    return {'contract': contract, 'dt': dt}


async def worker(name, queue):
    while True:
        log.debug(f'{name} started')
        task = await queue.get()
        next_chunk = await task()
        if next_chunk:
            await schedule_task(**next_chunk, queue=queue)
        queue.task_done()
        log.debug(f'{name} done')


async def main(contracts, number_of_workers, now=datetime.now()):

    if logmode=="logging.INFO":
        FileHandler(
            f'{__file__[:-3]}_{datetime.today().strftime("%Y-%m-%d_%H-%M")}', format_string='[{record.time:%y-%m-%d %H:%M:%S.%f%z}] {record.level_name}: {record.lineno}: {record.message}', bubble=True, delay=True).push_application()
        #logger=createLogger(__name__,args.log,logging.INFO)
    else:
        FileHandler(
            f'{__file__[:-3]}_{datetime.today().strftime("%Y-%m-%d_%H-%M")}', format_string='[{record.time:%y-%m-%d %H:%M:%S.%f%z}] {record.level_name}: {record.lineno}: {record.message}', bubble=True, delay=True).push_application()
        #logger=createLogger(__name__,args.log,logging.DEBUG)
        logger = logging.getLogger(__name__)
        #    logger=createLogger(__name__,"scan.log",logging.DEBUG)


    asyncio.get_event_loop().set_debug(True)

    log.debug('main function started')
    for i in contracts:
        log.debug(contracts)
    #log.debug('main function started'+contracts)
    #await ib.qualifyContractsAsync(*contracts)
    log.debug('contracts qualified')

    queue = asyncio.LifoQueue()
    producers = [asyncio.create_task(
        schedule_task(**initial_scheduleBF(c, backdays), queue=queue))
        for c in contracts]
    workers = [asyncio.create_task(worker(f'worker {i}', queue))
               for i in range(number_of_workers)]
    await asyncio.gather(*producers, return_exceptions=True)

    # wait until the queue is fully processed (implicitly awaits workers)
    await queue.join()

    # cancel all workers
    log.debug('cancelling workers')
    for w in workers:
        w.cancel()

    # wait until all worker tasks are cancelled
    await asyncio.gather(*workers, return_exceptions=True)


if __name__ == '__main__':

    # logging
    log = Logger(__name__)
    set_datetime_format("local")
    StreamHandler(sys.stdout, format_string='[{record.time:%y-%m-%d %H:%M:%S.%f%z}] {record.level_name}: {record.lineno}: {record.message}', bubble=True).push_application()
    #StreamHandler.format_string = '{record.lineno}: {record.message}'

    #StreamHandler.formatter
    FileHandler(
        f'{__file__[:-3]}_{datetime.today().strftime("%Y-%m-%d_%H-%M")}', format_string='[{record.time:%y-%m-%d %H:%M:%S.%f%z}] {record.level_name}: {record.lineno}: {record.message}', bubble=True, delay=True).push_application()

    log.debug(dbnameBF)
    symAll=[]
    for tbl in tbls.split(','):
        #symResult=myconnutils.select_from_db("symbol",tbl)
        #symbolList=selectSymFromTbl(tbl) 
        symbolList=myconnutils.select_from_db("symbol",tbl)
        symAll +=symbolList
    #log.debug(symAll)
        #symbolList=myconnutils.selectSymFromTbl("scantbl") 
    symList=list(set(symAll)) 
    log.debug(symList)

    #log.FileHandler
    #log.basicConfig(format='%(asctime)s %(filename)s:%(levelname)s line no: %(lineno)d %(message)s', filename='example.log',level=logging.DEBUG)

    # util.logToConsole()
    # get connection to Gateway
    #ib = IB_connection().ib
    ib=IB()
    ib.connect('127.0.0.1', 7499, clientId=19)

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # object where data is stored
    store = Store()

   # symbols = pd.read_csv(os.path.join(
   #     BASE_DIR, 'contracts.csv')).to_dict('records')
   # log.debug(symbols)
    # *lookup_contracts(symbols),
    contracts = [*lookup_contracts_Stock(symList)]
    #exit()
    number_of_workers = min(len(contracts), max_number_of_workers)
    ib.run(main(contracts, number_of_workers))
    log.debug('script finished, about to disconnect')
    ib.disconnect()
