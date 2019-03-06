import time
# from data_process.get_stock_pool import get_stocks
from backtest.tick_engine import TickEngine
from backtest.tick_evluation import Evaluation
from data_process.get_stock_params import get_stocks, get_trading_days
from data_process.base_define import trading_days_address


class StrategyMode(TickEngine):
    def __init__(self):
        super(StrategyMode, self).__init__()
        self.start_button = True                               # 策略启动按钮(为了区分是否第一次交易)
        self.strategy_start_day = None                         # 策略启动日期(记录第一次交易的时间)
        self.side_marker = ''                                  # 记录交易方向
        self.trans_number = 0                                  # 记录交易次数

    def reset_stock_pool(self):
        """重构选股"""
        return self.stock_list

    def strategy(self, stock, data):
        # ------------------------------------------------------------------
        # 交易两次
        # 1、当盘口出现买方的量是买方量的2倍时，买入；
        # 2、14:50前必须出场
        bid_ask_vol_spread = (data.bidV1 + data.bidV2 + data.bidV3 + data.bidV4 + data.bidV5 +
                              data.bidV6 + data.bidV7 + data.bidV8 + data.bidV9 + data.bidV10) / \
                              (data.askV1 + data.askV2 + data.askV3 + data.askV4 + data.askV5 +
                               data.askV6 + data.askV7 + data.askV8 + data.askV9 + data.askV10)
        # 初始交易，定义底仓
        if self.start_button:
            # 记录第一天的开始时间
            self.strategy_start_day = self.trading_days[0]
            for s in self.stock_list:
                lens = len(self.stock_list)
                self.weight[s] = 0.5/lens
                _amount = self.initial_cash * self.weight[s]
                _volume = self.volume_fix(self.tick[stock].close, _amount)
                self.buy(s, data.close, _volume)
            self.start_button = False
        else:
            # 每个交易日更新
            if self.current_day > self.strategy_start_day:
                # 当天第一次是买入的情况（价差达到2倍，且在14：50 之前发生第一次交易）
                if bid_ask_vol_spread >= 2 and self.button[stock] == 1 and\
                        data.date.strftime(self.time_format) < self.break_time:
                    self.button[stock] = 2
                    available_cash = self.available_cash
                    volume_exist = self.pos[stock]
                    self.vol[stock] = self.available_trade_volume(self.tick[stock].close, available_cash, volume_exist)
                    # 1、获取全局可用资金available_cash
                    # 2、获取需要做T的数量volume_exist
                    # 3、取上述两个值的最小值available_trade_volume(current_price, available_cash, volume_exist)
                    self.buy(stock, data.close, self.vol[stock])
                    self.side_marker = 'B'
                elif bid_ask_vol_spread <= 0.5 and self.button[stock] == 1 and\
                        data.date.strftime(self.time_format) < self.break_time:
                    self.button[stock] = 2
                    self.vol[stock] = self.pos[stock]
                    # 1、获取需要做T的数量volume_exist
                    self.sell(stock, data.close, self.vol[stock])
                    self.side_marker = 'S'
                
                # 第二次反向交易
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
    stock_list = ['SH600000', 'SZ000001']       #
    t0 = time.time()
    trading_black_days = ['2018-12-03', '2018-12-14', '2018-12-18', '2018-12-19', '2018-12-20',
                          '2018-12-21']
    trading_days = ["2019-02-13", "2019-02-14", "2019-02-15"]
    # trading_days = [x for x in get_trading_days(trading_days_address) if x not in trading_black_days]
    """回测"""
    engine = StrategyMode()
    engine.set_base_params(stock_list, trading_days)
    engine.back_test()
    """评估"""
    evaluate = Evaluation()
    evaluate.set_path(r'H:\jzquant_vnpy\backtest\strategy\trade_logs')
    evaluate.set_account_path(r'H:\jzquant_vnpy\backtest\strategy\trade_logs\account.csv')
    evaluate.print_result()
    evaluate.show()
    print('回测花销时间：', time.time()-t0)
