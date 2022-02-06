import pymongo
from eth_utils import from_wei
import math
import os
import sys
from datetime import datetime
from datetime import timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils
import meta

def profile(_address: str = None) -> dict:
  """
  Get profile
  """  
  with utils.dbInterface() as client:
    db = client['perp']
    criteria = {}
    if _address:
      criteria['_id'] = _address    
    cursor = db['traders'].find(criteria, limit = 1000).sort('timestamp', pymongo.DESCENDING)
    trader_list = []
    for trd in cursor:      
      trader_list.append({
        'trader': trd['_id'],
        'collateral': trd['collateral'],
        'cumTradingVolume': trd['tradingVolume'],
        'cumRealizedPnl': trd['realizedPnl'],
        'cumFundingPayment': trd['fundingPayment'],
        'cumTradingFee': trd['tradingFee'],
        'cumLiquidationFee': trd['liquidationFee'],
        'cumMakerFee': trd['makerFee'],
        'totalPnl': trd['totalPnl'],
        'cumBadDebt': trd['badDebt'],
        'blockNumber': trd['blockNumber'],
        'timestamp': trd['timestamp']
      })
    return trader_list

def badDebts(_trader: str = None,
             _start: int = None, _end: int = None) -> []:
  """
  Get bad debts
  """  
  with utils.dbInterface() as client:
    db = client['perp']
    criteria = {}
    if _trader:
      criteria['trader'] = _trader    
    if _start or _end:
      criteria['timestamp'] = {}
      if _start:
        criteria['timestamp']['$gte'] = _start
      if _end:
        criteria['timestamp']['$lte'] = _end  
    cursor = db['badDebts'].find(criteria, limit = 1000).sort('blockNumberLogIndex', pymongo.DESCENDING)
    debt_list = []
    for bd in cursor:      
      debt_list.append({
        # '_id': bd['id'],
        'txHash': bd['txHash'],
        'trader': bd['trader'],     
        'amount': bd['amount'],     
        'blockNumber': bd['blockNumber'],
        'timestamp': bd['timestamp']
      })
    return debt_list

def fetchTraderDayData(db, _trader: str):
  """
  Fetch trader day data
  """  
  query = """
  query traderDayData($trader: String!, $last_timestamp: BigInt!) {    
    traderDayData: traderDayDatas(      
      where: {
        trader: $trader,
        date_gt: $last_timestamp,
      },
      orderBy: date,
      orderDirection: asc,      
      first: 1000) {
        id        
        tradingVolume
        fee
        realizedPnl
        timestamp: date        
    }
  }
  """
  col_daydata = db['traderDayData']
  last_timestamp = 0
  if col_daydata.count_documents({'trader': _trader}) > 0:
    last_day_data = list(col_daydata.find(limit = 1).sort('_id', pymongo.DESCENDING))[0]
    last_timestamp = last_day_data['timestamp']
  while True:
    res = utils.performRequest(query, {
      'trader': _trader, 
      'last_timestamp': str(last_timestamp)
    })    
    if 'data' not in res:
      print(res)
      break
    data = res['data']
    day_data_list = []
    for dd in data['traderDayData']:
      sign = -1 if (dd['realizedPnl'][0] == '-') else 1
      day_data_list.append({
         '_id': dd['id'],
         'trader': _trader,
         'tradingVolume': float(dd['tradingVolume']),
         'realizedPnl': sign * float(from_wei(abs(int(dd['realizedPnl'])), 'ether')),
         'fee': float(from_wei(int(dd['fee']), 'ether')),
         'timestamp': int(dd['timestamp'])
      })
    if not day_data_list:
      print('nothing to be done.') 
      break
    last_timestamp = day_data_list[-1]['timestamp']
    last_id = day_data_list[-1]['_id']
    col_daydata.insert_many(day_data_list)
    print('read {} data points {}'.format(len(day_data_list), last_id))    
    if len(day_data_list) < 1000:
      print('trader day data is up to date.')
      break

def dayData(_trader: str,
            _start: int = None, _end: int = None) -> []:
  """
  Get day data
  """ 
  with utils.dbInterface() as client:
    db = client['perp']
    # update trader's day data
    fetchTraderDayData(db, _trader)

    criteria = {}
    if _trader:
      criteria['trader'] = _trader    
    if _start or _end:
      criteria['timestamp'] = {}
      if _start:
        criteria['timestamp']['$gte'] = _start
      if _end:
        criteria['timestamp']['$lte'] = _end  
    cursor = db['traderDayData'].find(criteria, limit = 1000).sort('timestamp', pymongo.DESCENDING)
    day_data_list = []
    for dd in cursor:      
      day_data_list.append({
        # '_id': dd['id'],
        'tradingVolume': dd['tradingVolume'],
        'realizedPnl': dd['realizedPnl'],
        'fee': dd['fee'],
        'timestamp': dd['timestamp']
      })
    return day_data_list