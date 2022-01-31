import pymongo
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils
  
def fetchLiquidations(db):
  """
  Fetch liquidations
  """  
  query = """
  query liquidations($last_log_index: BigInt!) {    
    liquidations: positionLiquidateds(    
      where: {
        blockNumberLogIndex_gt: $last_log_index
      },    
      orderBy: blockNumberLogIndex,
      orderDirection: asc,
      first: 1000) {
        id
        txHash
        trader
        baseToken
        liquidator
        positionSizeAbs
        positionNotionalAbs
        liquidationFee
        blockNumberLogIndex
        blockNumber
        timestamp
    }
  }
  """
  # get the last inserted liquidation
  col_liquidations = db['liquidation']
  liquidation_list = col_liquidations.find(limit = 1).sort('blockNumberLogIndex', pymongo.DESCENDING)
  last_log_index = liquidation_list[0]['blockNumberLogIndex'] if (col_liquidations.count_documents({}) > 0) else 0  
  print('starting log index: {}'.format(last_log_index))
  while True:
    res = utils.performRequest(query, {'last_log_index': str(last_log_index)})
    if not 'data' in res:
      print(res)
      break
    data = res['data']
    liquidation_list = [{
     '_id': liq['id'] ,
     'txHash': liq['txHash'],
     'trader': liq['trader'],
     'baseToken': liq['baseToken'],
     'liquidator': liq['liquidator'],
     'positionNotionalAbs': float(liq['positionNotionalAbs']),
     'positionSizeAbs': float(liq['positionSizeAbs']),
     'liquidationFee': float(liq['liquidationFee']),     
     'blockNumberLogIndex': int(liq['blockNumberLogIndex']),
     'blockNumber': int(liq['blockNumber']),
     'timestamp': int(liq['timestamp'])} for liq in data['liquidations']]  
    if not liquidation_list:
      print('nothing to be done.')
      break
    last_log_index = liquidation_list[-1]['blockNumberLogIndex']
    print('read {} data points, log_index: {}'.format(len(liquidation_list), last_log_index))
    col_liquidations.insert_many(liquidation_list)
    if len(liquidation_list) < 1000:
      print('liquidation data is up to date.')
      break

if __name__ == '__main__':
  with utils.dbInterface() as client:
    fetchLiquidations(client['perp'])