import pymongo
import math
import os
import sys
from datetime import datetime
from datetime import timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils
import meta

def liquidations(_trader: str = None, _asset_list: list = None,
                _liquidator: str = None,
                _start: int = None, _end: int = None) -> list:
  """
  Get liquidations
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
    if _liquidator:
      criteria['liquidator'] = _liquidator
    if _start or _end:
      criteria['timestamp'] = {}
      if _start:
        criteria['timestamp']['$gte'] = _start
      if _end:
        criteria['timestamp']['$lte'] = _end
    cursor = db['liquidation'].find(criteria, limit = 1000).sort('blockNumberLogIndex', pymongo.DESCENDING)
    liq_list = []
    for liq in cursor:
      liq_list.append({
        # '_id': liq['id'] ,
        'txHash': liq['txHash'],
        'trader': liq['trader'],
        'baseToken': liq['baseToken'],
        'asset': meta.Assets[liq['baseToken']],
        'liquidator': liq['liquidator'],
        'positionNotionalAbs': liq['positionNotionalAbs'],
        'positionSizeAbs': liq['positionSizeAbs'],
        'liquidationFee': liq['liquidationFee'],     
        'blockNumber': liq['blockNumber'],
        'timestamp': liq['timestamp']
      })
    return liq_list