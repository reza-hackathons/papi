import pymongo
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils

def fetchTrades(db):
  """
  Retrieve trades
  """  
  query = """
  query trades($from: BigInt!) {    
    trades: positionChangeds(    
      where: {
        blockNumberLogIndex_gt: $from
      },    
      orderBy: blockNumberLogIndex,
      orderDirection: asc,
      first: 1000) {
        id
        txHash
        trader
        baseToken
        exchangedPositionSize
        exchangedPositionNotional
        fee
        openNotional
        realizedPnl
        positionSizeAfter
        entryPriceAfter
        swappedPrice
        marketPriceAfter
        fromFunctionSignature
        blockNumberLogIndex
        blockNumber
        timestamp
    }
  }
  """
  col_trades = db['trades']
  trade_list = col_trades.find(limit = 1).sort('blockNumberLogIndex', pymongo.DESCENDING)
  last_log_index = trade_list[0]['blockNumberLogIndex'] if (col_trades.count_documents({}) > 0) else 0  
  print('starting log index: {}'.format(last_log_index))
  while True:
    res = utils.performRequest(query, {'from': str(last_log_index)})
    if not 'data' in res:
      print(res)
      break
    data = res['data']
    trade_list = [{
     '_id': trade['id'], 
     'txHash': trade['txHash'],
     'trader': trade['trader'],
     'baseToken': trade['baseToken'],
     'exchangedPositionSize': float(trade['exchangedPositionSize']),
     'exchangedPositionNotional': float(trade['exchangedPositionNotional']),
     'fee': float(trade['fee']),
     'openNotional': float(trade['openNotional']),
     'realizedPnl': float(trade['realizedPnl']),
     'positionSizeAfter': float(trade['positionSizeAfter']),
     'entryPriceAfter': float(trade['entryPriceAfter']),
     'swappedPrice': float(trade['swappedPrice']),
     'marketPriceAfter': float(trade['marketPriceAfter']),
     'fromFunctionSignature': trade['fromFunctionSignature'],
     'blockNumberLogIndex': int(trade['blockNumberLogIndex']),
     'blockNumber': int(trade['blockNumber']),
     'timestamp': int(trade['timestamp'])} for trade in data['trades']]  
    if not trade_list:
      print('nothing to be done.')
      break
    last_log_index = trade_list[-1]['blockNumberLogIndex']
    print('read {} data points, log_index: {}'.format(len(trade_list), last_log_index))
    col_trades.insert_many(trade_list)
    if len(trade_list) < 1000:
      print('trade data is up to date.')
      break

def fetchOpenPositions(db):
  """
  Fetch open positions
  """  
  query = """
  query openPositions($last_timestamp: BigInt!) {    
    openPositions: positions(      
      where: {
        timestamp_gte: $last_timestamp,
        # positionSize_not: 0
      }
      orderBy: timestamp,
      orderDirection: asc
      first: 1000) {
        id
        trader
        baseToken
        positionSize
        openNotional
        entryPrice
        tradingVolume
        realizedPnl
        fundingPayment
        tradingFee
        liquidationFee
        blockNumber
        timestamp    
    }
  }
  """
  col_openPositions = db['openPositions']   
  last_timestamp = 0
  if col_openPositions.count_documents({}) > 0:
    last_timestamp = col_openPositions.find(limit = 1).sort('timestamp', pymongo.DESCENDING)[0]['timestamp']
  print('starting timestamp: {}'.format(last_timestamp))
  while True:
    res = utils.performRequest(query, {'last_timestamp': str(last_timestamp)})    
    if 'data' not in res:
      print(res)
    data = res['data']        
    open_position_list = [{
     '_id': pos['id'],
     'trader': pos['trader'],
     'baseToken': pos['baseToken'],
     'positionSize': float(pos['positionSize']),
     'openNotional': float(pos['openNotional']),
     'entryPrice': float(pos['entryPrice']),
     'tradingVolume': float(pos['tradingVolume']),
     'realizedPnl': float(pos['realizedPnl']),
     'fundingPayment': float(pos['fundingPayment']),
     'tradingFee': float(pos['tradingFee']),
     'liquidationFee': float(pos['liquidationFee']),
     'blockNumber': int(pos['blockNumber']),
     'timestamp': int(pos['timestamp'])} for pos in data['openPositions']]
    if not open_position_list:
      print('nothing to be done.') 
      break
    last_timestamp = open_position_list[-1]['timestamp']
    print('read {} data points {}'.format(len(open_position_list), last_timestamp))
    for pos in open_position_list:
      criteria = {'_id': pos['_id']}  
      col_openPositions.replace_one(criteria, pos, upsert = True)
    if len(open_position_list) < 1000:
      print('open position data is up to date.')
      break
  
def fetchClosedPositions(db):
  """
  Fetch closed positions
  """  
  query = """
  query closedPositions($last_timestamp: BigInt!) {    
    closedPositions: positionHistories(      
      where: {
        timestamp_gte: $last_timestamp
      }
      orderBy: timestamp,
      orderDirection: asc
      first: 1000) {
        id
        trader
        baseToken
        positionSize
        openNotional
        entryPrice
        realizedPnl
        fundingPayment
        tradingFee
        liquidationFee
        blockNumber
        timestamp    
    }
  }
  """
  col_closedPositions = db['closedPositions']   
  last_timestamp = 0
  if col_closedPositions.count_documents({}) > 0:
    last_timestamp = col_closedPositions.find(limit = 1).sort('timestamp', pymongo.DESCENDING)[0]['timestamp']
  print('starting timestamp: {}'.format(last_timestamp))
  while True:
    res = utils.performRequest(query, {'last_timestamp': str(last_timestamp)})    
    if 'data' not in res:
      print(res)
    data = res['data']        
    closed_position_list = [{
     '_id': pos['id'],
     'trader': pos['trader'],
     'baseToken': pos['baseToken'],
     'positionSize': float(pos['positionSize']),
     'openNotional': float(pos['openNotional']),
     'entryPrice': float(pos['entryPrice']),
     'realizedPnl': float(pos['realizedPnl']),
     'fundingPayment': float(pos['fundingPayment']),
     'tradingFee': float(pos['tradingFee']),
     'liquidationFee': float(pos['liquidationFee']),
     'blockNumber': int(pos['blockNumber']),
     'timestamp': int(pos['timestamp'])} for pos in data['closedPositions']]
    if not closed_position_list:
      print('nothing to be done.')  
      break
    last_timestamp = closed_position_list[-1]['timestamp']
    print('read {} data points {}'.format(len(closed_position_list), last_timestamp))
    for pos in closed_position_list:
      criteria = {'_id': pos['_id']}  
      col_closedPositions.replace_one(criteria, pos, upsert = True)
    if len(closed_position_list) < 1000:
      print('closed position data is up to date.')
      break

if __name__ == '__main__':
  with utils.dbInterface() as client:
    fetchTrades(client['perp'])
    fetchOpenPositions(client['perp'])
    fetchClosedPositions(client['perp'])