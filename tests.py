import sys
import os 
import pprint

sys.path.append(os.path.join(os.path.dirname(__file__), 'api'))
import market
import position
import funding
import liquidation
import account
import order
import trader

if __name__ == '__main__':
  # tests
  # r = market.markets()
  # r = market.specs(['sol'])  
  # r = position.trades()
  # r = position.trades(_asset_list=['luna'])
  # r = position.trades(_trader = '0x1d16ea2b8bfcf5f7c13c1ea792d6c6a7a9aa1063')
  # r = position.trades(_trader = '0x1d16ea2b8bfcf5f7c13c1ea792d6c6a7a9aa1063', _asset_list = ['luna'])
  # r = position.openPositions(_asset_list=['btc', 'avax'])
  # r = position.closedPositions(_asset_list=['eth'])
  # r = position.closedPositions(_trader='0x6541e4d0f50a82544401b1e0721626415a37de99')
  # r = funding.updates(['crv', 'luna'], _start=1643715347)
  # r = funding.settlements(_asset_list=['eth', 'sol'], _start=1643670347, _end=1643674364)
  # r = funding.settlements()
  # r = liquidation.liquidations(_asset_list=['crv', 'sol', 'avax'])
  # r = liquidation.liquidations(_asset_list=['luna'])
  # r = account.deposits()
  # r = account.withdrawals()
  # r = order.openOrders(_asset_list=['crv', 'sol'])
  # r = order.openOrders(_asset_list=['btc'])
  # r = order.makers()
  # r = trader.info('0x1ca59dee56834035e35c7ad90c926d28667d9e20')
  # r = trader.badDebts(_start=1643398295)
  # r = trader.dayData(_trader='0xc9aef7014b9c65f567d8bc8c8b3218b0a1ec12b7', _start=1640304001)
  r = market.protocol()
  pp = pprint.PrettyPrinter()
  pp.pprint(r)