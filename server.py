from flask import Flask, jsonify, request, render_template
from flask_restful import Api, Resource
import json
import sys
import os 

sys.path.append(os.path.join(os.path.dirname(__file__), 'api'))
import market
import position
import funding
import liquidation
import account
import order
import trader


app = Flask(__name__)
api = Api(app)

@app.route('/')
@app.route('/v2')
def hello():
  return render_template('hello.html')

# overview and markets
@app.route('/v2/protocol', methods=['GET', 'POST'])
def getProtocol():  
  return jsonify({
    'result': market.protocol()
})

@app.route('/v2/markets', methods=['GET', 'POST'])
def getMarkets():
  payload = json.loads(request.data) if request.data else {}  
  result = market.markets(
    _asset_list = payload.get('assets')
  )
  return jsonify({
    'result': result
  })

@app.route('/v2/markets/specs', methods=['GET', 'POST'])
def getMarketSpecs():
  payload = json.loads(request.data) if request.data else {}  
  result = market.specs(
    _asset_list = payload.get('assets')
  )
  return jsonify({
    'result': result
  })

# positions, trades, ...
@app.route('/v2/position/trades', methods=['GET', 'POST'])
def getTrades():
  payload = json.loads(request.data) if request.data else {} 
  result = position.trades(
    _trader = payload.get('trader'),
    _asset_list = payload.get('assets'),
    _start = payload.get('start'),
    _end = payload.get('end')
  )
  return jsonify({
    'result': result
  })

@app.route('/v2/position/open', methods=['GET', 'POST'])
def getOpenPositions():
  payload = json.loads(request.data) if request.data else {} 
  result = position.openPositions(
    _trader = payload.get('trader'),
    _asset_list = payload.get('assets'),
    _start = payload.get('start'),
    _end = payload.get('end')
  )
  return jsonify({
    'result': result
  })

@app.route('/v2/position/closed', methods=['GET', 'POST'])
def getClosedPositions():
  payload = json.loads(request.data) if request.data else {} 
  result = position.closedPositions(
    _trader = payload.get('trader'),
    _asset_list = payload.get('assets'),
    _start = payload.get('start'),
    _end = payload.get('end')
  )
  return jsonify({
    'result': result
  })

# funding
@app.route('/v2/funding/updates', methods=['GET', 'POST'])
def getFundingUpdates():
  payload = json.loads(request.data) if request.data else {} 
  result = funding.updates(
    _asset_list = payload.get('assets'),
    _start = payload.get('start'),
    _end = payload.get('end')
  )
  return jsonify({
    'result': result
  })

@app.route('/v2/funding/payments', methods=['GET', 'POST'])
def getFundingPayments():
  payload = json.loads(request.data) if request.data else {} 
  result = funding.settlements(
    _trader = payload.get('trader'),
    _asset_list = payload.get('assets'),
    _start = payload.get('start'),
    _end = payload.get('end')
  )
  return jsonify({
    'result': result
  })

# liquidations
@app.route('/v2/liquidations', methods=['GET', 'POST'])
def getLiquidations():
  payload = json.loads(request.data) if request.data else {} 
  result = liquidation.liquidations(
    _trader = payload.get('trader'),
    _asset_list = payload.get('assets'),
    _liquidator = payload.get('liquidator'),
    _start = payload.get('start'),
    _end = payload.get('end')
  )
  return jsonify({
    'result': result
  })

# order
@app.route('/v2/order/open', methods=['GET', 'POST'])
def getOpenOrders():
  payload = json.loads(request.data) if request.data else {} 
  result = order.openOrders(
    _maker = payload.get('maker'),
    _asset_list = payload.get('assets'),
    _lower_tick = payload.get('lower_tick'),
    _upper_tick = payload.get('upper_tick'),
    _start = payload.get('start'),
    _end = payload.get('end')
  )
  return jsonify({
    'result': result
  })

@app.route('/v2/order/maker/profile', methods=['GET', 'POST'])
def getMakerStats():
  payload = json.loads(request.data) if request.data else {} 
  result = order.makers(
    _address = payload.get('maker'),    
  )
  return jsonify({
    'result': result
  })

# trader
@app.route('/v2/trader/profile', methods=['GET', 'POST'])
def getTraderProfile():
  payload = json.loads(request.data) if request.data else {} 
  result = trader.profile(
    _address = payload.get('trader'),
  )
  return jsonify({
    'result': result
  })

@app.route('/v2/trader/badDebts', methods=['GET', 'POST'])
def getTraderBadDebts():
  payload = json.loads(request.data) if request.data else {} 
  result = trader.badDebts(
    _trader = payload.get('trader'),
    _start = payload.get('start'),
    _end = payload.get('end')
  )
  return jsonify({
    'result': result
  })

@app.route('/v2/trader/dayData', methods=['GET', 'POST'])
def getTraderDayData():
  payload = json.loads(request.data) if request.data else {} 
  if 'trader' not in payload:
    return jsonify({
      'error': 'Trader address missing.'
    })
  result = trader.dayData(
    _trader = payload.get('trader'),
    _start = payload.get('start'),
    _end = payload.get('end')
  )
  return jsonify({
    'result': result
  })

# account
@app.route('/v2/account/deposits', methods=['GET', 'POST'])
def getDeposits():
  payload = json.loads(request.data) if request.data else {} 
  result = account.deposits(
    _trader = payload.get('trader'),
    _asset_list = payload.get('assets'),
    _start = payload.get('start'),
    _end = payload.get('end')
  )
  return jsonify({
    'result': result
  })

@app.route('/v2/account/withdrawals', methods=['GET', 'POST'])
def getWithdrawals():
  payload = json.loads(request.data) if request.data else {} 
  result = account.withdrawals(
    _trader = payload.get('trader'),
    _asset_list = payload.get('assets'),
    _start = payload.get('start'),
    _end = payload.get('end')
  )
  return jsonify({
    'result': result
  })

if __name__ == '__main__':
  app.run(debug = True)

