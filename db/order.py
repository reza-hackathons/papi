import pymongo
from eth_utils import from_wei
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils

def fetchOpenOrders(db):
  """
  Fetch open orders
  """  
  query = """
  query openOrders($last_timestamp: BigInt!) {
    openOrders: openOrders(      
      where: {
        timestamp_gte: $last_timestamp,
        # liquidity_gt: 0
      }
      orderBy: timestamp,
      orderDirection: asc
      first: 1000) {
        id
        maker
        baseToken
        lowerTick
        upperTick
        baseAmount
        quoteAmount
        liquidity
        collectedFee
        collectedFeeInThisLifecycle
        blockNumber
        timestamp    
    }
  }
  """
  col_openOrders = db['openOrders']   
  last_timestamp = 0
  if col_openOrders.count_documents({}) > 0:
    last_timestamp = col_openOrders.find(limit = 1).sort('timestamp', pymongo.DESCENDING)[0]['timestamp']
  print('starting timestamp: {}'.format(last_timestamp))
  while True:
    res = utils.performRequest(query, {'last_timestamp': str(last_timestamp)})    
    if 'data' not in res:
      print(res)
    data = res['data']        
    open_order_list = [{
     '_id': order['id'],
     'maker': order['maker'],
     'baseToken': order['baseToken'],
     'lowerTick': float(int(order['lowerTick'])),
     'upperTick': float(int(order['upperTick'])),
     'baseAmount': float(order['baseAmount']),
     'quoteAmount': float(order['quoteAmount']),
     'liquidity': float(from_wei(abs(int(order['liquidity'])), 'ether')),
     'collectedFee': float(order['collectedFee']),
     'collectedFeeInThisLifecycle': float(order['collectedFeeInThisLifecycle']),
     'blockNumber': int(order['blockNumber']),
     'timestamp': int(order['timestamp'])} for order in data['openOrders']]
    if not open_order_list:
      print('nothing to be done.') 
      break
    last_timestamp = open_order_list[-1]['timestamp']
    print('read {} data points {}'.format(len(open_order_list), last_timestamp))
    for order in open_order_list:
      criteria = {'_id': order['_id']}  
      col_openOrders.replace_one(criteria, order, upsert = True)
    if len(open_order_list) < 1000:
      print('open order data is up to date.')
      break

def fetchMakers(db):
  """
  Fetch makers
  """  
  query = """
  query makers($last_timestamp: BigInt!) {    
    makers: makers(      
      where: {
        timestamp_gte: $last_timestamp,
      }
      orderBy: timestamp,
      orderDirection: asc
      first: 1000) {
        id
        collectedFee
        blockNumber
        timestamp    
    }
  }
  """
  col_makers = db['makers']   
  last_timestamp = 0
  if col_makers.count_documents({}) > 0:
    last_timestamp = col_makers.find(limit = 1).sort('timestamp', pymongo.DESCENDING)[0]['timestamp']
  print('starting timestamp: {}'.format(last_timestamp))
  while True:
    res = utils.performRequest(query, {'last_timestamp': str(last_timestamp)})    
    if 'data' not in res:
      print(res)
    data = res['data']        
    maker_list = [{
     '_id': maker['id'],
     'collectedFee': float(maker['collectedFee']),
     'blockNumber': int(maker['blockNumber']),
     'timestamp': int(maker['timestamp'])} for maker in data['makers']]
    if not maker_list:
      print('nothing to be done.') 
      break
    last_timestamp = maker_list[-1]['timestamp']
    print('read {} data points {}'.format(len(maker_list), last_timestamp))
    for maker in maker_list:
      criteria = {'_id': maker['_id']}  
      col_makers.replace_one(criteria, maker, upsert = True)
    if len(maker_list) < 1000:
      print('maker data is up to date.')
      break

if __name__ == '__main__':
  with utils.dbInterface() as client:
    fetchOpenOrders(client['perp'])    
    fetchMakers(client['perp'])
