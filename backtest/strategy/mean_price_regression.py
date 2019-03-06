"""
# 策略逻辑：
1、计算黄色的平均成本线
2、偏离度为3%
3、趋势定义，近N小时涨跌5%
4、止盈：2%
5、14：45之后停止交易
"""

import time
# from data_process.get_stock_pool import get_stocks
from backtest.tick_engine import TickEngine


class StrategyMode(TickEngine):
    def __init__(self):
        super(StrategyMode, self).__init__()
        self.start_button = True                               # 策略启动按钮
        self.strategy_start_day = None                         # 策略启动日期
        self.side_marker = ''                                  # 记录交易方向
        self.trans_number = 0                                  # 记录交易次数

    def get_current_return(self, data, current_time, current_price, count):
        # 查找N小时前的价格
        t = current_time - count
        former_price = data.loc[t, 'close']
        return current_price / former_price - 1

    def strategy(self, stock, data):
        # ------------------------------------------------------------------
        # 交易两次
        # 1、计算黄色的平均成本线
        cum_money = data.money
        cum_volume = data.volume
        yellow_cost = cum_money / cum_volume




        if self.start_button:
            # 记录第一天的开始时间
            self.strategy_start_day = self.trading_days[0]
            for s in self.stock_list:
                lens = len(self.stock_list)
                self.weight[s] = 0.5/lens
                _amount = self.initial_cash * self.weight[s]
                _volume = self.volume_limit(self.tick[stock].close, _amount)
                self.buy(s, data.close, _volume)
            self.start_button = False
        else:
            if self.current_day > self.strategy_start_day:
                # 当天第一次是买入的情况
                if bid_ask_vol_spread >= 2 and self.button[stock] == 1:
                    self.button[stock] = 2
                    available_cash = self.available_cash
                    volume_exist = self.pos[stock]
                    self.vol[stock] = self.available_trade_volume(self.tick[stock].close, available_cash, volume_exist)
                    # 1、获取全局可用资金available_cash
                    # 2、获取需要做T的数量volume_exist
                    # 3、取上述两个值的最小值available_trade_volume(current_price, available_cash, volume_exist_T)
                    self.buy(stock, data.close, self.vol[stock])
                    self.side_marker = 'B'
                elif bid_ask_vol_spread <= 0.5 and self.button[stock] == 1:
                    self.button[stock] = 2
                    self.vol[stock] = self.pos[stock]
                    # 1、获取需要做T的数量volume_exist_T
                    self.sell(stock, data.close, self.vol[stock])
                    self.side_marker = 'S'

                # 当天第一次是卖出的情况
                if data.date.strftime(self.time_format) >= self.break_time and \
                        self.button[stock] == 2 and self.side_marker == 'B':
                    self.button[stock] = 0
                    self.side_marker = ''
                    self.sell(stock, data.close, self.vol[stock])
                elif data.date.strftime(self.time_format) >= self.break_time and\
                        self.button[stock] == 2 and self.side_marker == 'S':
                    self.button[stock] = 0
                    self.side_marker = ''
                    self.buy(stock, data.close, self.vol[stock])


if __name__ == '__main__':

    # stock_list = get_stocks(r'H:\vnpyStockEngine\data_process\sh_stock_list.json')
    stock_list = ['SH600000', 'SH600004']       #
    t0 = time.time()
    trading_black_days = ['2018-12-03', '2018-12-14', '2018-12-18']
    trading_days = ['2018-12-04', '2018-12-05', '2018-12-06', '2018-12-07', '2018-12-10',
                    '2018-12-11', '2018-12-12', '2018-12-13']
    engine = StrategyMode()
    engine.set_base_params(stock_list, trading_days)
    engine.back_test()

    print('回测花销时间：', time.time()-t0)
