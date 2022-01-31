import sys
import os 
import pprint

sys.path.append(os.path.join(os.path.dirname(__file__), 'api'))
import market
import position


if __name__ == '__main__':
  # tests
  # r = market.market('btc')
  # r = market.markets()
  # r = market.markets(['crv'])
  # r = market.marketSpecs('sol')  
  # r = position.trades()
  # r = position.trades(asset = 'luna')
  # r = position.trades(trader = '0x1d16ea2b8bfcf5f7c13c1ea792d6c6a7a9aa1063')
  # r = position.trades(trader = '0x1d16ea2b8bfcf5f7c13c1ea792d6c6a7a9aa1063', asset = 'luna')
  # r = position.openPositions(asset='luna')
  # r = position.closedPositions(asset='eth')
  r = position.closedPositions(trader='0x6541e4d0f50a82544401b1e0721626415a37de99')
  pp = pprint.PrettyPrinter()
  pp.pprint(r)