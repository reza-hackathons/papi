import pymongo
import math
import os
import sys
from datetime import datetime
from datetime import timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils
import meta

def openOrders(_maker: str = None, _asset_list: list = None,
               _lower_tick: int = None, _upper_tick: int = None,
               _start: int = None, _end: int = None) -> list:
  """
  Get open orders
  """  
  with utils.dbInterface() as client:
    db = client['perp']
    criteria = {
      'liquidity': {'$ne': 0}
    }
    if _maker:
      criteria['maker'] = _maker
    if _asset_list:
      _asset_list = [meta.assetNameToAddress(asset) for asset in _asset_list]
      _asset_list = [asset for asset in _asset_list if asset]
      if _asset_list:
        criteria['baseToken'] = {'$in': _asset_list}    
    if _lower_tick:      
      criteria['lowerTick'] = {'$gte': _lower_tick}
    if _upper_tick:
      criteria['upperTick'] = {'$lte': _upper_tick}
    if _start or _end:
      criteria['timestamp'] = {}
      if _start:
        criteria['timestamp']['$gte'] = _start
      if _end:
        criteria['timestamp']['$lte'] = _end      
    cursor = db['openOrders'].find(criteria, limit = 1000).sort('timestamp', pymongo.DESCENDING)
    order_list = []
    for order in cursor:      
      order_list.append({
        # '_id': order['id'],
        'maker': order['maker'],
        'baseToken': order['baseToken'],
        'asset': meta.Assets[order['baseToken']],
        'lowerTick': order['lowerTick'],
        'upperTick': order['upperTick'],
        'baseAmount': order['baseAmount'],
        'quoteAmount': order['quoteAmount'],
        'liquidity': order['liquidity'],
        'collectedFee': order['collectedFee'],
        'collectedFeeInThisLifecycle': order['collectedFeeInThisLifecycle'],
        'blockNumber': order['blockNumber'],
        'timestamp': order['timestamp']
      })
    return order_list

def makers(_address: str = None) -> dict:
  """
  Get makers
  """  
  with utils.dbInterface() as client:
    db = client['perp']    
    criteria = {}
    if _address:
      criteria['maker'] = _address    
    cursor = db['makers'].find(criteria, limit = 1000).sort('timestamp', pymongo.DESCENDING)
    maker_list = []
    for mk in cursor:      
      maker_list.append({
        'maker': mk['_id'],
        'collectedFee': mk['collectedFee'],
        'blockNumber': mk['blockNumber'],
        'timestamp': mk['timestamp']
      })
    return maker_list