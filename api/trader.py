import pymongo
import math
import os
import sys
from datetime import datetime
from datetime import timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils
import meta

def info(_address: str = None) -> dict:
  """
  Get info
  """  
  with utils.dbInterface('185.221.237.140') as client:
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
  with utils.dbInterface('185.221.237.140') as client:
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