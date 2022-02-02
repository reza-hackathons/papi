import pymongo
import math
import os
import sys
from datetime import datetime
from datetime import timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils
import meta

def deposits(_trader: str = None, _asset_list: list = None,
             _start: int = None, _end: int = None) -> list:
  """
  Get deposits
  """  
  with utils.dbInterface('185.221.237.140') as client:
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
    cursor = db['deposits'].find(criteria, limit = 1000).sort('blockNumberLogIndex', pymongo.DESCENDING)
    deposit_list = []
    for dep in cursor:
      deposit_list.append({
        # '_id': dep['id'],
       'txHash': dep['txHash'],
       'trader': dep['trader'],     
       'collateralToken': dep['collateralToken'],
       'collateralName': meta.Assets[dep['collateralToken']],
       'amount': dep['amount'],     
       'blockNumber': dep['blockNumber'],
       'timestamp': dep['timestamp']
      })
    return deposit_list

def withdrawals(_trader: str = None, _asset_list: list = None,
                _start: int = None, _end: int = None) -> list:
  """
  Get withdrawals
  """  
  with utils.dbInterface('185.221.237.140') as client:
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
    cursor = db['withdrawals'].find(criteria, limit = 1000).sort('blockNumberLogIndex', pymongo.DESCENDING)
    withdrawal_list = []
    for wid in cursor:
      withdrawal_list.append({
        # '_id': dep['id'],
       'txHash': wid['txHash'],
       'trader': wid['trader'],     
       'collateralToken': wid['collateralToken'],
       'collateralName': meta.Assets[wid['collateralToken']],
       'amount': wid['amount'],     
       'blockNumber': wid['blockNumber'],
       'timestamp': wid['timestamp']
      })
    return withdrawal_list

