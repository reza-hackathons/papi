import pymongo
import math
import os
import sys
from datetime import datetime
from datetime import timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils
import meta

def trades(_trader: str = None, _asset_list: list = None,
           _start: int = None, _end: int = None) -> list:
  """
  get trades
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
    trade_list = []
    cursor = db['trades'].find(criteria, limit = 1000).sort('blockNumberLogIndex', pymongo.DESCENDING)
    for trade in cursor:
      trade_list.append({              
       'txHash': trade['txHash'],
       'trader': trade['trader'],
       'baseToken': trade['baseToken'],
       'asset': meta.Assets[trade['baseToken']],
       'exchangedPositionSize': math.fabs(trade['exchangedPositionSize']),
       'exchangedPositionNotional': math.fabs(trade['exchangedPositionNotional']),
       'side': 'short' if trade['exchangedPositionSize'] < 0 else 'long',
       'fee': trade['fee'],
       'openNotional': math.fabs(trade['openNotional']),
       'realizedPnl': trade['realizedPnl'],
       'positionSizeAfter': trade['positionSizeAfter'],
       'entryPriceAfter': trade['entryPriceAfter'],
       'swappedPrice': trade['swappedPrice'],
       'marketPriceAfter': trade['marketPriceAfter'],
       'timestamp': trade['timestamp']
      })
    return trade_list

def openPositions(_trader: str = None, _asset_list: list = None,
                  _start: int = None, _end: int = None) -> list:
  """
  Get open positions
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
    criteria['positionSize'] = {'$ne': 0}
    pos_list = []
    cursor = db['openPositions'].find(criteria, limit = 1000).sort('timestamp', pymongo.DESCENDING)
    for pos in cursor:
      pos_list.append({
        # '_id': pos['id'],
        'trader': pos['trader'],
        'baseToken': pos['baseToken'],
        'asset': meta.Assets[pos['baseToken']],
        'positionSize': math.fabs(pos['positionSize']),
        'openNotional': math.fabs(pos['openNotional']),
        'side': 'short' if pos['positionSize'] < 0 else 'long',
        'entryPrice': pos['entryPrice'],
        'cumTradingVolume': pos['tradingVolume'],
        'cumRealizedPnl': pos['realizedPnl'],
        'cumFundingPayment': pos['fundingPayment'],
        'cumFees': pos['tradingFee'],
        'cumLiquidationFee': pos['liquidationFee'],
        'blockNumber': pos['blockNumber'],
        'timestamp': pos['timestamp']
       })
    return pos_list

def closedPositions(_trader: str = None, _asset_list: list = None,
                    _start: int = None, _end: int = None) -> list:
  """
  Get closed positions
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
    # criteria['positionSize'] = {'$ne': 0}
    pos_list = []
    cursor = db['closedPositions'].find(criteria, limit = 1000).sort('timestamp', pymongo.DESCENDING)
    for pos in cursor:
      pos_list.append({
        # '_id': pos['id'],
        'trader': pos['trader'],
        'baseToken': pos['baseToken'],
        'asset': meta.Assets[pos['baseToken']],
        'positionSize': math.fabs(pos['positionSize']),
        'openNotional': math.fabs(pos['openNotional']),
        'side': 'short' if pos['positionSize'] < 0 else 'long',
        'entryPrice': pos['entryPrice'],
        'realizedPnl': pos['realizedPnl'],
        'fundingPayment': pos['fundingPayment'],
        'fee': pos['tradingFee'],
        'liquidationFee': pos['liquidationFee'],
        'blockNumber': pos['blockNumber'],
        'timestamp': pos['timestamp']
       })
    return pos_list
