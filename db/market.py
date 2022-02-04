from pymongo import MongoClient
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils

def fetchProtocols(db):
  """
  Retrieve protocols
  """
  query = """
    query fetchProtocols {
      protocols: protocols (
        first: 1000
      ){
          id
          network
          chainId
          contractVersion
          publicMarketCount
          tradingVolume
          tradingFee
          totalValueLocked
          badDebt
          blockNumber
          timestamp    
      }
    }
  """
  res = utils.performRequest(query)
  if not 'data' in res:
    print(res)
    return
  data = res['data']
  protocol_list = [{
   'pid': proto['id'],
   'network': proto['network'],
   'chainId': proto['chainId'],
   'contractVersion': proto['contractVersion'],   
   'publicMarketCount': int(proto['publicMarketCount']),
   'tradingVolume': float(proto['tradingVolume']),
   'tradingFee': float(proto['tradingFee']),
   'totalValueLocked': float(proto['totalValueLocked']),
   'badDebt': float(proto['badDebt']),
   'blockNumber': int(proto['blockNumber']),
   'timestamp': int(proto['timestamp'])} for proto in data['protocols']]
  if not protocol_list:
    print('nothing to be done.')
    return
  last_timestamp = protocol_list[-1]['timestamp']
  print(f'read {len(protocol_list)} data points, last timestamp: {last_timestamp}')
  # db
  col_markets = db['protocols']     
  col_markets.insert_many(protocol_list)
  if len(protocol_list) < 1000:
    print('protocl data is up to date.')
    return  
  
def fetchMarkets(db):
  """
  Retrieve markets
  """
  query = """
    query fetchMarkets {
      markets: markets (
        first: 1000
      ){
          baseToken
          quoteToken
          pool
          feeRatio
          tradingVolume
          tradingFee
          baseAmount
          quoteAmount
          blockNumberAdded
          timestampAdded
          blockNumber
          timestamp    
      }
    }
  """
  res = utils.performRequest(query)
  if not 'data' in res:
    print(res)
    return
  data = res['data']
  market_list = [{
   'baseToken': market['baseToken'],
   'quoteToken': market['quoteToken'],
   'pool': market['pool'],   
   'feeRatio': int(market['feeRatio']), # 500 (0.05%), 3000 (0.3%), 10000 (1%)
   'tradingVolume': float(market['tradingVolume']),
   'tradingFee': float(market['tradingFee']),
   'baseAmount': float(market['baseAmount']),
   'quoteAmount': float(market['quoteAmount']),
   'timestampAdded': int(market['timestampAdded']),
   'blockNumberAdded': int(market['blockNumberAdded']),
   'blockNumber': int(market['blockNumber']),
   'timestamp': int(market['timestamp'])} for market in data['markets']]
  if not market_list:
    print('nothing to be done.')
    return
  last_timestamp = market_list[-1]['timestamp']
  print(f'read {len(market_list)} data points, last timestamp: {last_timestamp}')
  # db
  col_markets = db['markets']     
  col_markets.insert_many(market_list)
  if len(market_list) < 1000:
    print('market data is up to date.')
    return

if __name__ == '__main__':
  with utils.dbInterface() as client:
    fetchMarkets(client['perp'])
    fetchProtocols(client['perp'])

