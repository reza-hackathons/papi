import pymongo
import math
import os
import sys
from datetime import datetime
from datetime import timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils
import meta

def protocol() -> dict:
  """
  get protocol
  """
  with utils.dbInterface('185.221.237.140') as client:
    db = client['perp']
    
    proto_list = cursor = list(db['protocols'].find(limit = 1).sort('timestamp', pymongo.DESCENDING))
    if not proto_list:
      return {}
    active_markets = [ma for ma in meta.ReverseAssets.keys() if 'usd' not in ma.lower()] 
    proto = proto_list[0]      
    return {
      'id': proto['pid'],
      'network': proto['network'],
      'chainId': proto['chainId'],
      'contractVersion': proto['contractVersion'],   
      'activeMArkets': active_markets,
      'tradingVolume': proto['tradingVolume'],
      'tradingFee': proto['tradingFee'],
      'tvl': proto['totalValueLocked'],
      'badDebt': proto['badDebt'],
      'blockNumber': proto['blockNumber'],
      'timestamp': proto['timestamp']
    }


def market(_asset: str = None) -> dict:
  """
  Get market info
  """  
  new_asset = meta.assetNameToAddress(_asset)
  if not new_asset:
    return {
      'error': f'Invalid asset name or address: {_asset}. Available: {meta.availableAssets()}'
    }
  utc_now = datetime.utcnow()
  _1h_ago = math.ceil((utc_now - timedelta(hours = 1)).timestamp())  
  _24h_ago = math.ceil((utc_now - timedelta(days = 1)).timestamp())  
  
  trades24h = []
  funding_rate = 0
  open_interest, open_value = (0, 0)
  with utils.dbInterface('185.221.237.140') as client:
    db = client['perp']
    criteria = {
      'baseToken': new_asset,
      'timestamp': {'$gte': _24h_ago, '$lte': math.ceil(utc_now.timestamp())}
    }
    projection = ['baseToken', 'pool', 'marketPriceAfter',
                  'exchangedPositionSize', 'exchangedPositionNotional', 'timestamp']
    # change list to cursor for performance gains
    trades24h = list(db['trades'].find(filter = criteria, projection = projection).sort('blockNumberLogIndex', pymongo.DESCENDING))
    last_funding = list(db['fundingUpdates'].find({'baseToken': new_asset}, limit = 1).sort('blockNumberLogIndex', pymongo.DESCENDING))   
    # funding 
    funding_rate = last_funding[0]['dailyFundingRate'] if last_funding else 0 
    # open interest
    criteria = {
      'baseToken': new_asset,
      'positionSize': {'$ne': 0}
    }
    projection = ['positionSize', 'openNotional']
    open_positions = db['openPositions'].find(filter = criteria, projection = projection)        
    for pos in open_positions:
      open_interest += math.fabs(pos['openNotional'])
      open_value += math.fabs(pos['positionSize'])

  if not trades24h:    
    return {
      'error': f'{meta.Assets[new_asset]}: not enough data.'
    }
  # volume and changes   
  # print(f'size: {len(trades24h)}, from {datetime.fromtimestamp(trades24h[-1]["timestamp"])} to {datetime.fromtimestamp(trades24h[-0]["timestamp"])}')
  base_vol24h, quote_vol24h = (0.0, 0.0)
  market_price = trades24h[0]['marketPriceAfter']      
  price_24h_ago = trades24h[-1]['marketPriceAfter']    
  price_1h_ago = 1
  found_price_1h_ago = False
  shorts_vol24 = 0
  low_24h, high_24h = (sys.float_info.max, -1)
  for trade in trades24h:
    base_vol24h += math.fabs(trade['exchangedPositionSize'])
    quote_amount = trade['exchangedPositionNotional']
    quote_vol24h += math.fabs(quote_amount)
    shorts_vol24 += math.fabs(quote_amount) if (quote_amount > 0) else 0
    mp = trade['marketPriceAfter']
    low_24h = low_24h if (low_24h < mp) else mp
    high_24h = high_24h if (high_24h > mp) else mp
    if trade['timestamp'] < _1h_ago:
      found_price_1h_ago = True
      # print('found 1h ago: {}'.format(mp))
    if not found_price_1h_ago:
      price_1h_ago = mp
  change1h = market_price / price_1h_ago
  change1h = (change1h - 1) if (change1h > 1) else -(1 - change1h)
  change24h = market_price / price_24h_ago
  change24h = (change24h - 1) if (change24h > 1) else -(1 - change24h)
  return { 
    'asset': meta.Assets[new_asset],
    'baseToken': trades24h[0]['baseToken'],
    'marketPrice': market_price,
    'high24h': high_24h,
    'low24h': low_24h,
    'baseVolume24h': base_vol24h,
    'quoteVolume24h': quote_vol24h,
    'change1h': change1h,
    'change24': change24h,
    'sl24h': shorts_vol24 / quote_vol24h,
    'openInterest': open_interest,
    'openValue': open_value,
    'fundingRate': funding_rate,
    'timestamp': trades24h[0]['timestamp'] 
  }

def markets(_asset_list: list = []) -> list:
  """
  Get requested markets
  """
  _asset_list = [meta.assetNameToAddress(asset) for asset in _asset_list]
  _asset_list = [asset for asset in _asset_list if asset]
  if not _asset_list:
    _asset_list = [asset for asset in meta.Assets.keys() if asset != meta.ReverseAssets['usd']]
  market_list = [market(asset) for asset in _asset_list]
  return market_list

def specs(_asset_list: list = None) -> list:
  """
  Get overall spec about a market
  """  
  if _asset_list:
    _asset_list = [meta.assetNameToAddress(asset) for asset in _asset_list]
    _asset_list = [asset for asset in _asset_list if asset]    
  with utils.dbInterface('185.221.237.140') as client:
    db = client['perp']
    criteria = {}
    if _asset_list:
      criteria['baseToken'] = {'$in': _asset_list}    
    cursor = db['markets'].find(filter = criteria, limit = 1).sort('timestamp', pymongo.DESCENDING)
    spec_list = []
    for spec in cursor:
      spec_list.append({
        'asset': meta.Assets[spec['baseToken']],
        'baseToken': spec['baseToken'],
        'quoteToken': spec['quoteToken'],
        'poolAddress': spec['pool'],
        'feeRatio': spec['feeRatio'] / 10000.0, #@ check with the devs
        'totalVolume': spec['tradingVolume'],
        'totalFees': spec['tradingFee'],
        'baseLiquidity': spec['baseAmount'],
        'quoteLiquidity': spec['quoteAmount'],
        'introduced': spec['timestampAdded'],
        'timestamp': spec['timestamp']
      })
    return spec_list