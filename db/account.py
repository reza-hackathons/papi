import pymongo
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils

def fetchDeposits(db):
  """
  Retrieve deposits
  """  
  query = """
  query deposits($from: BigInt!) {    
    deposits: depositeds(    
      where: {
        blockNumberLogIndex_gt: $from
      },    
      orderBy: blockNumberLogIndex,
      orderDirection: asc,
      first: 1000) {
        id
        txHash
        trader
        collateralToken
        amount
        blockNumberLogIndex
        blockNumber
        timestamp
    }
  }
  """
  col_deposits = db['deposits']
  deposit_list = col_deposits.find(limit = 1).sort('blockNumberLogIndex', pymongo.DESCENDING)
  last_log_index = deposit_list[0]['blockNumberLogIndex'] if (col_deposits.count_documents({}) > 0) else 0  
  print('starting log index: {}'.format(last_log_index))
  while True:
    res = utils.performRequest(query, {'from': str(last_log_index)})
    if not 'data' in res:
      print(res)
      break
    data = res['data']
    deposit_list = [{
     '_id': dep['id'],
     'txHash': dep['txHash'],
     'trader': dep['trader'],     
     'collateralToken': dep['collateralToken'],
     'amount': float(dep['amount']),     
     'blockNumberLogIndex': int(dep['blockNumberLogIndex']),
     'blockNumber': int(dep['blockNumber']),
     'timestamp': int(dep['timestamp'])} for dep in data['deposits']]  
    if not deposit_list:
      print('nothing to be done.')
      break
    last_log_index = deposit_list[-1]['blockNumberLogIndex']
    print('read {} data points, log_index: {}'.format(len(deposit_list), last_log_index))
    col_deposits.insert_many(deposit_list)
    if len(deposit_list) < 1000:
      print('deposit data is up to date.')
      break

def fetchWithdrawals(db):
  """
  Retrieve withdrawals
  """  
  query = """
  query withdrawals($from: BigInt!) {    
    withdrawals: withdrawns(    
      where: {
        blockNumberLogIndex_gt: $from
      },    
      orderBy: blockNumberLogIndex,
      orderDirection: asc,
      first: 1000) {
        id
        txHash
        trader
        collateralToken
        amount
        blockNumberLogIndex
        blockNumber
        timestamp
    }
  }
  """
  col_withdrawals = db['withdrawals']
  withdrawal_list = col_withdrawals.find(limit = 1).sort('blockNumberLogIndex', pymongo.DESCENDING)
  last_log_index = withdrawal_list[0]['blockNumberLogIndex'] if (col_withdrawals.count_documents({}) > 0) else 0  
  print('starting log index: {}'.format(last_log_index))
  while True:
    res = utils.performRequest(query, {'from': str(last_log_index)})
    if not 'data' in res:
      print(res)
      break
    data = res['data']
    withdrawal_list = [{
     '_id': wid['id'],
     'txHash': wid['txHash'],
     'trader': wid['trader'],     
     'collateralToken': wid['collateralToken'],
     'amount': float(wid['amount']),     
     'blockNumberLogIndex': int(wid['blockNumberLogIndex']),
     'blockNumber': int(wid['blockNumber']),
     'timestamp': int(wid['timestamp'])} for wid in data['withdrawals']]  
    if not withdrawal_list:
      print('nothing to be done.')
      break
    last_log_index = withdrawal_list[-1]['blockNumberLogIndex']
    print('read {} data points, log_index: {}'.format(len(withdrawal_list), last_log_index))
    col_withdrawals.insert_many(withdrawal_list)
    if len(withdrawal_list) < 1000:
      print('withdrawal data is up to date.')
      break

if __name__ == '__main__':
  with utils.dbInterface() as client:
    fetchDeposits(client['perp'])    
    fetchWithdrawals(client['perp'])