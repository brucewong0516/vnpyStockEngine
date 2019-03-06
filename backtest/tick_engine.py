import os
import pandas as pd
import datetime
from collections import defaultdict, OrderedDict

import backtest.object as vo
from data_process.get_stock_history import get_min_data
from data_process.log_auth import client
from data_process.get_stock_tick import get_tick, get_wash_tick, get_data_filter_by_time


class TickEngine(object):

    def __init__(self):
        # ------------------------------------------------------------------
        # 交易标的和交易时间
        self.stock_list = []
        self.trading_days = []
        self.trading_begin = '9:30:00'
        self.trading_end = '15:00:00'
        self.time_format = '%Y-%m-%d %H:%M:%S'
        self.current_day = None
        # ------------------------------------------------------------------
        #  交易市场和交易数据类型
        self.market = 'SH'
        self.market_number = 1
        self.trade_buy = 'buy'
        self.trade_sell = 'sell'
        self.rate_commission = 0.0003
        self.rate_slippage = 0.0003
        # ------------------------------------------------------------------
        # 账户信息(包含K线序列,保证金序列,现金序列等)
        self.total_account = None
        self.account = defaultdict(None, {})          # 账户信息对象
        self.freeze_account = defaultdict(lambda: 0)  # 账户冻结信息
        self.freeze_pos = defaultdict(lambda: 0)   # 账户冻结数量
        self.pos_last = defaultdict(lambda: 0)        # 上一根tick走完后的持仓量
        self.close_last = defaultdict(lambda: 0)      # 上一根tick走完后的收盘价
        # ------------------------------------------------------------------
        # 仓位、权重和初始资金
        self.pos = defaultdict(lambda: 0)
        self._positionPnl = defaultdict(lambda: 0)
        self.weight = defaultdict(lambda: 0)
        self.initial_cash = 5000
        self.available_cash = 5000
        # ------------------------------------------------------------------
        # tick数据
        self.tick = defaultdict(None, {})
        self.dt = defaultdict(None, {})
        # ------------------------------------------------------------------
        # 订单和成交单计数器
        self.orderCount = defaultdict(lambda: 0)
        self.tradeCount = defaultdict(lambda: 0)
        # ------------------------------------------------------------------
        # 订单字典
        self.OrderDict = defaultdict(dict, {})
        self.workingOrderDict = defaultdict(dict, {})
        # ------------------------------------------------------------------
        # 成交单字典
        self.vol = defaultdict(lambda: 0)
        self.tradeDict = defaultdict(dict, {})
        self.workingTradeList = defaultdict(list, {})
        # ------------------------------------------------------------------
        # 日线结果字典
        self.dailyResultDict = OrderedDict()
        # ------------------------------------------------------------------
        # 设定全局的开关
        self.initial = True
        self.button = defaultdict(lambda: 0)
        # ------------------------------------------------------------------
        # 设定全局的严格出场时间
        self.break_time = None
        self.break_time_minute = '14:50:00'
        # 设定交易的收益、手续费、滑点、交易持仓收益
        # ------------------------------------------------------------------
        self.tradingPnl = 0
        self.commission = 0
        self.slippage = 0
        self.positionPnl = 0
        # 设定存储路径
        # ------------------------------------------------------------------
        self.trade_log_path = './trade_logs'
        # 如果不存在该路径则创建
        if not os.path.exists(self.trade_log_path):
            os.makedirs(self.trade_log_path)
        # 定义交易存储路径
        self.logTradeFile = self.trade_log_path + os.sep + 'trade.csv'
        self.logAccountTempFile = self.trade_log_path + os.sep + 'account_temp.csv'
        self.logAccountFile = self.trade_log_path + os.sep + 'account.csv'
        # 设定存储记录的变量
        self.tradeRecordsDf = None
        self.accountRecordsDf = None

    def set_base_market(self, market, market_number):
        """设定市场参数"""
        # ------------------------------------------------------------------
        self.market = market
        self.market_number = market_number

    def set_base_params(self, stockList, tradingDays):
        """设定交易时间和股池"""
        # ------------------------------------------------------------------
        self.stock_list = stockList
        self.trading_days = tradingDays

    def reset_time_format(self, time_format):
        self.time_format = time_format

    def reset_trade_params(self):
        """设定交易统计参数"""
        # ------------------------------------------------------------------
        self.tradingPnl = 0
        self.commission = 0
        self.slippage = 0
        self.positionPnl = 0

    def reset_break_minute(self, t):
        """设定出场时间"""
        # ------------------------------------------------------------------
        self.break_time_minute = t

    def reset_stock_pool(self):
        """重新更新股池"""
        return self.stock_list

    def get_last_ticks(self, stock, startTime, endTime):
        """获取tick数据的接口,回测引擎专用"""
        # ------------------------------------------------------------------
        if 'SH' in stock:
            self.market = 'SH'
            df = get_tick(client, startTime, endTime, stock, 1)
            df = get_wash_tick(df, market='SH')
        elif 'SZ' in stock:
            self.market = 'SH'
            df = get_tick(client, startTime, endTime, stock, 2)
            df = get_wash_tick(df, market='SZ')
        df = get_data_filter_by_time(df)
        return df

    def get_last_mins(self, stock, startTime, endTime, count):
        """获取某个交易日内的freq数据"""
        # 1、访问存储在内存中的数据，每次便利股票池时，更新该变量
        # 2、重新访问数据接口，需提供股票和时间
        return get_min_data(stock, startTime, endTime, count=count)

    def buy(self, stock, price, volume):
        """买开"""
        # ------------------------------------------------------------------
        # 根据可用资金及当时的价格进行判断是否可以开仓
        if price * volume <= self.available_cash:
            return self.send_order(stock, self.trade_buy, price, volume)
        else:
            print("Not Enough Money")
            return self.send_order(stock, self.trade_buy, price, 0)

    def sell(self, stock, price, volume):
        """平仓"""
        # ------------------------------------------------------------------
        # 根据可平仓数量进行判断
        if self.pos[stock] >= volume:
            return self.send_order(stock, self.trade_sell, price, volume)
        else:
            print("Not Enough {}".format(stock))
            return self.send_order(stock, self.trade_sell, price, 0)

    def send_order(self, stock, order_type, price, volume):
        """发送限价单，主要是创建订单详情"""
        # ------------------------------------------------------------------
        # 委托编号
        self.orderCount[stock] += 1
        orderID = str(self.orderCount[stock])
        """创建委托单对象"""
        order = vo.OrderData()               # 订单数据类
        order.symbol = stock                 # 订单代码
        order.orderID = orderID              # 订单ID
        order.price = price                  # 订单价格，一般是bar的收盘价
        order.volume = volume                # 订单成交量
        order.orderTime = self.dt            # 订单提交时间
        order.offset = order_type            # 委托类型映射,添加order的方向
        # 保存到限价单字典中
        self.workingOrderDict[stock][orderID] = order
        # 此时冻结账户部分可用资金,只有买入的方向需要冻结
        self.freeze_account[stock] = order.price * order.volume
        # 此时冻结账户可以交易的数量,只有卖出的方向需要冻结
        self.freeze_pos[stock] = order.volume
        if order.offset == self.trade_buy:
            self.pos[stock] = self.pos[stock]
            self.available_cash = self.available_cash - self.freeze_account[stock]
        elif order.offset == self.trade_sell:
            self.pos[stock] = self.pos[stock] - self.freeze_pos[stock]
            self.available_cash = self.available_cash
        # 返回委托代码
        return orderID

    def cross_order(self, stock):
        """交易接口"""
        # ------------------------------------------------------------------
        # 当前存在的限价单循环
        for orderID, order in list(self.workingOrderDict[stock].items()):

            # order.price是上一个bar收盘价
            buyCross = order.offset == self.trade_buy and self.tick[stock].close
            sellCross = order.offset == self.trade_sell and self.tick[stock].close
            # 如果发生了成交
            if buyCross or sellCross:
                # 推送成交数据
                self.tradeCount[stock] += 1              # 成交编号自增1
                tradeID = str(self.tradeCount[stock])
                """创建成交单对象"""
                trade = vo.TradeData()
                trade.symbol = order.symbol              # 记录交易标的
                trade.tradeID = tradeID                  # 记录交易编号
                trade.orderID = order.orderID            # 记录订单编号
                trade.offset = order.offset              # 记录执行交易的类型
                trade.price = self.tick[stock].close     # 以当前tick的价格成交
                # trade.price = order.price                # 以委托单价格成交

                # 对冻结信息进行解冻
                if buyCross:
                    self.pos[stock] += order.volume      # 更新仓位
                    self.available_cash = self.available_cash + self.freeze_account[stock]
                else:
                    self.pos[stock] += self.freeze_pos[stock]
                    self.pos[stock] -= order.volume
                trade.volume = order.volume              # 更新已成交量
                # 更新成交单的时间,以当前的tick的时间为成交时间
                trade.tradeTime = self.tick[stock].date.strftime(self.time_format)
                # 将成交单添加到成交字典和成交总表
                self.tradeDict[stock][tradeID] = trade
                self.workingTradeList[stock].append(trade)
                print(orderID, trade.__dict__)
                # 从总限价单列表删除该已成交orderID的限价单
                if orderID in self.workingOrderDict[stock].keys():
                    del self.workingOrderDict[stock][orderID]

    def updateTickAccount(self, stock):
        """更新账户的信息"""
        # ------------------------------------------------------------------
        posChange = 0.0  # 变动数量
        if len(self.workingTradeList[stock]):
            for trade in self.workingTradeList[stock]:
                vol = abs(trade.volume)
                # 开仓的情况
                if trade.offset == self.trade_buy:
                    posChange = vol
                # 平仓的情况
                elif trade.offset == self.trade_sell:
                    posChange = -vol
                # 计算交易收益，其实在当个tick时的交易不产生收益
                self.tradingPnl += round(abs(posChange) * (self.tick[stock].close - trade.price), 2)
                self.commission += round(trade.price * vol * self.rate_commission, 2)
                self.slippage += round(vol * trade.price * self.rate_slippage, 2)
                # available_cash实时进行开扣减的参数
                _commission = round(trade.price * vol * self.rate_commission, 2)
                _slippage = round(vol * trade.price * self.rate_slippage, 2)
                self._positionPnl[stock] = round(posChange * self.tick[stock].close, 2)
        else:
            _commission = 0
            _slippage = 0
            self._positionPnl[stock] = 0
        # 持仓收益：计算上一个bar结束时持仓的部分到当前bar走完时的持仓收益
        self.positionPnl += round(self.pos_last[stock] * (self.tick[stock].close - self.close_last[stock]), 2)
        # 实时计算可用资金
        # 可用资金 = 可用资金 - 交易手续费和交易滑点 + 收益
        self.available_cash = round(self.available_cash - self._positionPnl[stock] - _commission - _slippage, 2)
        # 更新为上一根tick的数据
        self.close_last[stock] = self.tick[stock].close
        self.pos_last[stock] = self.pos[stock]
        # 删除计算过的trade字典
        self.workingTradeList[stock] = []
        # 删除冻结的信息
        self.freeze_pos[stock] = 0
        self.freeze_account[stock] = 0

    def on_tick(self, stock, startTime, endTime):
        """最底层的回测调用函数"""
        # 单日单股票
        # ------------------------------------------------------------------
        # 每天重新开启开关
        self.button[stock] = 1
        current_day = startTime.strftime(self.time_format).split(' ')[0]
        # 设定强制出场时间
        self.break_time = current_day + ' ' + self.break_time_minute
        try:
            # 获取单日单股票的tick数据
            tick_data = self.get_last_ticks(stock, startTime, endTime)
            tick_data.drop_duplicates(subset=['date'], keep='first', inplace=True)
            # 声明TickData的容器
            dataClass = vo.TickData
            # 每个交易日每个股票的初始tradingPnl、commission、slippage、positionPnl均重新初始化为0
            self.reset_trade_params()
            # 最外层的时间循环
            for t in list(tick_data.index.values):
                data = dataClass()
                data.__dict__ = tick_data.loc[t, :].to_dict()
                self.tick[stock] = data                     # 最新tick对象
                self.dt = t                                 # 最新tick线时间(使用tick结束时间)
                self.cross_order(stock)                     # 撮合成交,只有产生trade后才会进行撮合判断
                # 更新账户信息，每次循环tick时，更新成交收益(该tick)和持仓收益(相对上一个tick)
                self.updateTickAccount(stock)
                self.strategy(stock, data)                  # 推送tick到策略中
            # 交易日结束后更新最终的收益持仓等数据
            self.updateDayAccount(stock)
        except Exception as e:
            pass

    def back_test(self):
        """多股票回测"""
        # ------------------------------------------------------------------
        # 实例化总账户
        self.total_account = vo.MultiAccountData(initCapital=self.initial_cash)
        with open(self.logAccountTempFile, 'w') as f:
            f.write('')
        for date in self.trading_days:
            print('当前日期：', date)
            self.current_day = date
            # 设定交易时间的日交易区间
            startTime = datetime.datetime.strptime(date + ' ' + self.trading_begin, self.time_format)
            endTime = datetime.datetime.strptime(date + ' ' + self.trading_end, self.time_format)
            # 根据策略选股更新股池
            self.stock_list = self.reset_stock_pool()
            """遍历股池"""
            for stk in self.stock_list:
                # 生成stock的子账户
                self.account[stk] = vo.MultiAccountData()
                # 每日初始更新当日成交量为0
                self.vol[stk] = 0
                self.on_tick(stk, startTime, endTime)
            # 计算每日结果
            self.updateDailyResult(date)
        # 计算成交记录
        self.get_trade_records()
        # 计算账户记录
        self.get_account_records(self.logAccountTempFile)
        os.remove(self.logAccountTempFile)

    def updateDayAccount(self, stock):
        totalPnl = self.tradingPnl + self.positionPnl
        netPnl = round(totalPnl - self.commission - self.slippage, 2)
        # 子账户信息暂存
        self.account[stock].pos = self.pos[stock]                      # 持续跟踪交易标的的交易情况
        self.account[stock].totalPnl = round(totalPnl, 2)              # totalPnl = tradingPnl + positionPnl
        self.account[stock].commission = round(self.commission, 2)     # 此处记录的只是当次成交的手续费
        self.account[stock].slippage = round(self.slippage, 2)         # 此处记录的只是当次成交的滑点
        self.account[stock].netPnl = round(netPnl, 2)                  # 此处记录的只是当次成交的净利润

    def updateDailyResult(self, date):
        """更新每日的资金"""
        # 每日将股票遍历完了进行统计
        # ------------------------------------------------------------------
        if date not in self.dailyResultDict:
            self.dailyResultDict[date] = vo.DailyMultiResult(date)
        # 统计每日交易结果
        totalPnl = [(key, values.totalPnl) for key, values in self.account.items()]
        commission = [(key, values.commission) for key, values in self.account.items()]
        slippage = [(key, values.slippage) for key, values in self.account.items()]
        netPnl = [(key, values.netPnl) for key, values in self.account.items()]
        pos = [(key, values.pos) for key, values in self.account.items() if values.pos != 0]

        # 更新子账户表
        self.dailyResultDict[date].update(pos, totalPnl, commission, slippage, netPnl)
        # print(self.dailyResultDict[date].__dict__)

        # 更新总账户
        self.total_account.date = date
        self.total_account.pos = pos
        self.total_account.totalPnl = round(sum([x[1] for x in totalPnl]), 2)
        self.total_account.commission = round(sum([x[1] for x in commission]), 2)
        self.total_account.slippage = round(sum([x[1] for x in slippage]), 2)
        self.total_account.netPnl = round(sum([x[1] for x in netPnl]), 2)
        self.total_account.capital += round(self.total_account.netPnl, 2)
        self.total_account.available = round(self.available_cash, 2)
        # self.total_account.available = self.calculate_available_cash()
        with open(self.logAccountTempFile, 'a') as f:
            f.write(str(self.total_account.__dict__) + str('\n'))

    def calculate_available_cash(self):
        """不能实时计算可用资金，只能显示收盘时的情况"""
        current_pos_values = 0
        try:
            for kv in self.total_account.pos:
                current_pos_values += self.tick[kv[0]].close * kv[1]
            self.total_account.available = self.total_account.capital - current_pos_values
        except Exception as e:
            self.total_account.available = self.total_account.capital
        return self.total_account.available

    def get_account_records(self, file):
        with open(file, 'r') as f:
            ff = f.read()
        ff = [eval(x) for x in ff.split('\n')[:-1]]
        df = pd.DataFrame(ff)
        df.sort_values(by=['date'], ascending=True, inplace=True)
        df.to_csv(self.logAccountFile)
        self.accountRecordsDf = df
        return df

    def get_trade_records(self):
        # 统计交易记录
        trade_list = []
        for k, v in self.tradeDict.items():
            for kk, vv in v.items():
                trade_list.append(vv.__dict__)
        trade_df = pd.DataFrame(trade_list)
        trade_df.index = trade_df['symbol']
        trade_df.drop(['symbol'], axis=1, inplace=True)
        trade_df.sort_values(by=['tradeTime'], inplace=True, ascending=True)
        self.tradeRecordsDf = trade_df
        trade_df.to_csv(self.logTradeFile)
        return trade_df

    def volume_fix(self, current_price, money):
        """根据资金确定成交数量"""
        return int(money / (current_price * 100)) * 100

    def available_trade_volume(self, current_price, money, volume):
        """可供交易的量"""
        try:
            return min(self.volume_fix(current_price, money), volume)
        except Exception as e:
            raise e

    def strategy(self, stock, data):
        """策略逻辑函数"""
        # 在策略文件中重构
        pass
