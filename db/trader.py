import pymongo
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils

def fetchTraders(db):
  """
  Fetch traders
  """  
  query = """
  query traders($last_timestamp: BigInt!) {    
    traders: traders(      
      where: {
        timestamp_gte: $last_timestamp,
      }
      orderBy: timestamp,
      orderDirection: asc
      first: 1000) {
        id
        collateral
        tradingVolume
        realizedPnl
        fundingPayment
        tradingFee
        liquidationFee
        makerFee
        totalPnl
        badDebt
        blockNumber
        timestamp    
    }
  }
  """
  col_traders = db['traders']   
  last_timestamp = 0
  if col_traders.count_documents({}) > 0:
    last_timestamp = col_traders.find(limit = 1).sort('timestamp', pymongo.DESCENDING)[0]['timestamp']
  print('starting timestamp: {}'.format(last_timestamp))
  while True:
    res = utils.performRequest(query, {'last_timestamp': str(last_timestamp)})    
    if 'data' not in res:
      print(res)
    data = res['data']        
    trader_list = [{
     '_id': tr['id'],
     'collateral': float(tr['collateral']),
     'tradingVolume': float(tr['tradingVolume']),
     'realizedPnl': float(tr['realizedPnl']),
     'fundingPayment': float(tr['fundingPayment']),
     'tradingFee': float(tr['tradingFee']),
     'liquidationFee': float(tr['liquidationFee']),
     'makerFee': float(tr['makerFee']),
     'totalPnl': float(tr['totalPnl']),
     'badDebt': float(tr['badDebt']),
     'blockNumber': int(tr['blockNumber']),
     'timestamp': int(tr['timestamp'])} for tr in data['traders']]
    if not trader_list:
      print('nothing to be done.') 
      break
    last_timestamp = trader_list[-1]['timestamp']
    print('read {} data points {}'.format(len(trader_list), last_timestamp))
    for tr in trader_list:
      criteria = {'_id': tr['_id']}  
      col_traders.replace_one(criteria, tr, upsert = True)
    if len(trader_list) < 1000:
      print('trader data is up to date.')
      break

def fetchTraderDayData(db):
  """
  Fetch trader day data
  """  
  query = """
  query traderDayData($last_timestamp: BigInt!) {    
    traderDayData: traderDayDatas(      
      where: {
        date_gte: $last_timestamp,
      }
      orderBy: date,
      orderDirection: asc
      first: 1000) {
        id
        trader {
          id
        }
        tradingVolume
        fee
        realizedPnl
        timestamp: date        
    }
  }
  """
  col_traderDayData = db['traderDayData']   
  last_timestamp = 0
  if col_traderDayData.count_documents({}) > 0:
    last_timestamp = col_traderDayData.find(limit = 1).sort('timestamp', pymongo.DESCENDING)[0]['timestamp']
  print('starting timestamp: {}'.format(last_timestamp))
  while True:
    res = utils.performRequest(query, {'last_timestamp': str(last_timestamp)})    
    if 'data' not in res:
      print(res)
      break
    data = res['data']        
    day_data_list = [{
     '_id': dd['id'],
     'trader': dd['trader']['id'],
     'tradingVolume': float(dd['tradingVolume']),
     'realizedPnl': dd['realizedPnl'],
     'fee': dd['fee'],
     'timestamp': int(dd['timestamp'])} for dd in data['traderDayData']]
    if not day_data_list:
      print('nothing to be done.') 
      break
    last_timestamp = day_data_list[-1]['timestamp']
    print('read {} data points {}'.format(len(day_data_list), last_timestamp))
    for dd in day_data_list:
      criteria = {'_id': dd['_id']}  
      col_traderDayData.replace_one(criteria, dd, upsert = True)
    if len(day_data_list) < 1000:
      print('trader day data is up to date.')
      break

def fetchBadDebts(db):
  """
  Retrieve bad debts' occurences
  """  
  query = """
  query badDebts($last_log_index: BigInt!) {    
    badDebts: badDebtHappeneds(    
      where: {
        blockNumberLogIndex_gt: $last_log_index
      },    
      orderBy: blockNumberLogIndex,
      orderDirection: asc,
      first: 1000) {
        id
        txHash
        trader
        amount
        blockNumberLogIndex
        blockNumber
        timestamp
    }
  }
  """
  col_badDebts = db['badDebts']
  bad_debt_list = col_badDebts.find(limit = 1).sort('blockNumberLogIndex', pymongo.DESCENDING)
  last_log_index = bad_debt_list[0]['blockNumberLogIndex'] if (col_badDebts.count_documents({}) > 0) else 0  
  print('starting log index: {}'.format(last_log_index))
  while True:
    res = utils.performRequest(query, {'last_log_index': str(last_log_index)})
    if not 'data' in res:
      print(res)
      break
    data = res['data']
    bad_debt_list = [{
     '_id': bd['id'],
     'txHash': bd['txHash'],
     'trader': bd['trader'],     
     'amount': float(bd['amount']),     
     'blockNumberLogIndex': int(bd['blockNumberLogIndex']),
     'blockNumber': int(bd['blockNumber']),
     'timestamp': int(bd['timestamp'])} for bd in data['badDebts']]  
    if not bad_debt_list:
      print('nothing to be done.')
      break
    last_log_index = bad_debt_list[-1]['blockNumberLogIndex']
    print('read {} data points, log_index: {}'.format(len(bad_debt_list), last_log_index))
    col_badDebts.insert_many(bad_debt_list)
    if len(bad_debt_list) < 1000:
      print('bad debt data is up to date')
      break

if __name__ == '__main__':
  with utils.dbInterface() as client:
    fetchTraders(client['perp'])    
    # fetchTraderDayData(client['perp'])
    fetchBadDebts(client['perp'])
