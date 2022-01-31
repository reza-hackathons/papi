from pymongo import MongoClient
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils
  
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

