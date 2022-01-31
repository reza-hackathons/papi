import pymongo
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils
  
def fetchFundingUpdates(db):
  """
  Fetch funding updates
  """  
  query = """
  query fundingUpdates($from: BigInt!) {    
    fundingUpdates: fundingUpdateds(    
      where: {
        blockNumberLogIndex_gt: $from
      },    
      orderBy: blockNumberLogIndex,
      orderDirection: asc,
      first: 1000) {
        id
        baseToken
        markTwap
        indexTwap
        dailyFundingRate
        txHash
        blockNumberLogIndex
        blockNumber
        timestamp
    }
  }
  """
  col_fundingUpdates = db['fundingUpdates']
  funding_list = col_fundingUpdates.find(limit = 1).sort('blockNumberLogIndex', pymongo.DESCENDING)
  last_log_index = funding_list[0]['blockNumberLogIndex'] if (col_fundingUpdates.count_documents({}) > 0) else 0  
  print('starting log index: {}'.format(last_log_index))
  while True:
    data = utils.performRequest(query, {'from': str(last_log_index)})['data']

    funding_list = [{
     '_id': funding['id'], 
     'txHash': funding['txHash'],
     'baseToken': funding['baseToken'],
     'markTwap': float(funding['markTwap']),
     'indexTwap': float(funding['indexTwap']),
     'dailyFundingRate': float(funding['dailyFundingRate']),     
     'blockNumberLogIndex': int(funding['blockNumberLogIndex']),
     'blockNumber': int(funding['blockNumber']),
     'timestamp': int(funding['timestamp'])} for funding in data['fundingUpdates']]  
    if not funding_list:
      print('nothing to be done.')
      break
    last_log_index = funding_list[-1]['blockNumberLogIndex']
    print('read {} data points, log_index: {}'.format(len(funding_list), last_log_index))
    col_fundingUpdates.insert_many(funding_list)
    if len(funding_list) < 1000:
      print('funding data is up to date.')
      break

def fetchFundingSettlements(db):
  """
  Fetch funding settlements
  """  
  query = """
  query fundingSettlements($from: BigInt!) {    
    fundingSettlements: fundingPaymentSettleds(    
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
        fundingPayment
        blockNumberLogIndex
        blockNumber
        timestamp
    }
  }
  """
  # get the last inserted funding
  col_fundings = db['fundingSettlements']
  funding_list = col_fundings.find(limit = 1).sort('blockNumberLogIndex', pymongo.DESCENDING)
  last_log_index = funding_list[0]['blockNumberLogIndex'] if (col_fundings.count_documents({}) > 0) else 0  
  print('starting log index: {}'.format(last_log_index))
  while True:
    data = utils.performRequest(query, {'from': str(last_log_index)})['data']
    funding_list = [{
     '_id': funding['id'],
     'txHash': funding['txHash'],
     'trader': funding['trader'],
     'baseToken': funding['baseToken'],
     'fundingPayment': float(funding['fundingPayment']),
     'blockNumberLogIndex': int(funding['blockNumberLogIndex']),
     'blockNumber': int(funding['blockNumber']),
     'timestamp': int(funding['timestamp'])} for funding in data['fundingSettlements']]  
    if not funding_list:
      print('nothing to be done.')
      break
    last_log_index = funding_list[-1]['blockNumberLogIndex']
    print('read {} data points, log_index: {}'.format(len(funding_list), last_log_index))
    col_fundings.insert_many(funding_list)
    if len(funding_list) < 1000:
      print('funding settlement data is up to date.')
      break

if __name__ == '__main__':
  with utils.dbInterface() as client:
    fetchFundingUpdates(client['perp'])
    fetchFundingSettlements(client['perp'])
