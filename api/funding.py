import pymongo
import math
import os
import sys
from datetime import datetime
from datetime import timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils
import meta

def updates(_asset_list: list = None,
            _start: int = None, _end: int = None) -> list:
  """
  Get funding updates(changes)
  """    
  with utils.dbInterface() as client:
    db = client['perp']
    criteria = {}    
    if _asset_list:
      _asset_list = [meta.assetNameToAddress(asset) for asset in _asset_list]
      _asset_list = [asset for asset in _asset_list if asset]
      if _asset_list:
        criteria['baseToken'] = {'$in': _asset_list}
    if _start or _end:
      criteria['timestamp'] = {}
      if _start:
        criteria['timestamp']['$gte'] = _start
      if _end:
        criteria['timestamp']['$lte'] = _end
    cursor = db['fundingUpdates'].find(criteria, limit = 1000).sort('blockNumberLogIndex', pymongo.DESCENDING)
    funding_list = []
    for funding in cursor:
      funding_list.append({
       # '_id': funding['id'], 
       'txHash': funding['txHash'],
       'baseToken': funding['baseToken'],
       'name': meta.Assets[funding['baseToken']],
       'markTwap': funding['markTwap'],
       'indexTwap': funding['indexTwap'],
       'dailyFundingRate': funding['dailyFundingRate'],     
       'blockNumber': funding['blockNumber'],
       'timestamp': funding['timestamp']
      })
    return funding_list

def settlements(_trader: str = None, _asset_list: list = None,
                _start: int = None, _end: int = None) -> list:
  """
  Get funding settlements(payments)
  """  
  with utils.dbInterface() as client:
    db = client['perp']
    criteria = {}
    if _trader:
      criteria['trader'] = _trader
    if _asset_list:
      _asset_list = [meta.assetNameToAddress(asset) for asset in _asset_list]
      _asset_list = [asset for asset in _asset_list if asset]
      if _asset_list:
        criteria['baseToken'] = {'$in': _asset_list}
    if _start or _end:
      criteria['timestamp'] = {}
      if _start:
        criteria['timestamp']['$gte'] = _start
      if _end:
        criteria['timestamp']['$lte'] = _end
    cursor = db['fundingSettlements'].find(criteria, limit = 1000).sort('blockNumberLogIndex', pymongo.DESCENDING)
    funding_list = []
    for funding in cursor:
      funding_list.append({
       # '_id': funding['id'], 
       'txHash': funding['txHash'],
       'trader': funding['trader'],
       'baseToken': funding['baseToken'],
       'name': meta.Assets[funding['baseToken']],
       'fundingPayment': funding['fundingPayment'],         
       'blockNumber': funding['blockNumber'],
       'timestamp': funding['timestamp']
      })
    return funding_list