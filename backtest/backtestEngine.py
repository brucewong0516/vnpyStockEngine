# -*- coding: utf-8 -*-
"""
回测引擎, 实时计算模式, 只保存日线的资金序列
"""
import os
import time
from datetime import datetime, timedelta
from collections import OrderedDict
from itertools import product
import multiprocessing
import pymongo
import pandas as pd
# 导入自定义的模块
from backtest import constant as vc         # 常量模块
from backtest import object as vo           # 对象模块
from backtest.evaluation import Evaluation    # 评价指标计算对象


class BacktestingEngine(object):

    TICK_MODE = 'tick'  # 数据模式，逐Tick回测
    BAR_MODE = 'bar'  # 数据模式，逐Bar回测

    # ----------------------------------------------------------------------
    def __init__(self):
        # ------------------------------------------------------------------
        # 策略实例相关
        self.strategy = None            # 回测策略实例
        self.strategy_name = ""         # 策略文件夹名称, 如: "Strategy_DoubleMA1_20180101_101515"

        # ------------------------------------------------------------------
        # 回测时间相关
        self.startDate = ''            # 回测开始日期, "20180101", 由用户通过set输入
        self.endDate = ''              # 回测结束日期, "20181101", 由用户通过set输入
        self.initDays = 0              # 策略开始前准备数据时间, int, 由用户通过set输入
        self.dataStartDate = None      # 回测数据开始日期，datetime对象
        self.dataEndDate = None        # 回测数据结束日期，datetime对象, 计算得出
        self.strategyStartDate = None  # 策略启动日期（即前面的数据用于初始化），datetime对象, 计算得出

        # ------------------------------------------------------------------
        # 交易参数相关(全都需要用户输入)
        self.initCapital = 1000000              # 期初资金
        self.fixed_slippage = 0.0               # 固定数额滑点
        self.rate_slippage = 0.001              # 比例滑点
        self.rate_commission = 0.0003           # 手续费比例(为小数)
        self.fixCommission = 5                  # 手续费固定

        # ------------------------------------------------------------------
        # 交易品种频率相关
        self.strategyInfo = {}          # 策略信息字典
        self.market = ""                # 品种所在的市场
        self.symbol = ""                # 交易标的
        self.frequency = ""             # 交易的时间周期, 1min,30min,1hour,2hour,1day

        # ------------------------------------------------------------------
        # 数据库相关
        self.ipAddress = "localhost"    # 数据库IP地址
        self.port = 27017               # 数据库端口
        self.dbName = ''                # 回测数据库名, 如 "crypto_30min"
        self.dbClient = None            # 数据库客户端
        self.initData = []              # 初始化用的数据

        # ------------------------------------------------------------------
        # 委托单相关
        self.limitOrderCount = 0                    # 限价单编号计数
        self.limitOrderDict = OrderedDict()         # 限价单字典(全部委托单)
        self.workingLimitOrderDict = OrderedDict()  # 活动限价单字典(只保留当前的)

        # ------------------------------------------------------------------
        # 成交相关
        self.tradeCount = 0             # 成交编号
        self.tradeDict = OrderedDict()  # 成交字典, key为成交编号, value为成交对象

        # ------------------------------------------------------------------
        # 实时K线信息
        self.bar = None                 # 最新的bar
        self.dt = None                  # 最新的时间(datatime2)

        # ------------------------------------------------------------------
        # 账户信息(包含K线序列,保证金序列,现金序列等)
        self.account = None             # 账户信息对象
        self.pos_last = 0               # 上一根K线走完后的持仓量
        self.close_last = 0.0           # 上一根K线走完后的收盘价
        self.workingTradeList = []      # 成交对象

        # ------------------------------------------------------------------
        # 日志, 结果保存路径相关
        self.strategySavingPath = ""    # 策略结果保存路径

        # ------------------------------------------------------------------
        # 回测评价指标相关
        self.evaluator = None                 # 评价指标计算对象
        self.dailyResultDict = OrderedDict()  # 日线结果字典

    # ----------------------------------------------------------------------
    # 策略初始化相关设置函数
    # ----------------------------------------------------------------------
    def setDatabase(self, address="localhost", port=27017):
        """设置历史数据所用的数据库"""
        self.ipAddress = address                             # 数据库IP地址
        self.port = port                                     # 数据库端口
        self.dbClient = pymongo.MongoClient(address, port)

    # ----------------------------------------------------------------------
    def setStartDate(self, startDate='20100416', initDays=10):
        """设置回测的启动日期，注意此时需要多准备initDays的数据"""
        self.startDate = startDate  # 策略开始运行日期,"%Y%m%d"
        self.initDays = initDays    # 策略初始化回放天数,int
        # 将数据转化为datetime格式
        # 策略开始运行的时间（即输入的时间）
        self.strategyStartDate = datetime.strptime(startDate, '%Y%m%d')
        # 数据准备时间(往前推initDays天)
        self.dataStartDate = self.strategyStartDate - timedelta(initDays)

    # ----------------------------------------------------------------------
    def setEndDate(self, endDate=''):
        """设置回测的结束日期，如果设定结束日，以传入的结束日为基础，否则就是当前日"""
        self.endDate = endDate
        if endDate:
            self.dataEndDate = datetime.strptime(endDate, '%Y%m%d')
            # 若不修改时间则会导致不包含dataEndDate当天数据
            self.dataEndDate.replace(hour=23, minute=59)
        else:
            self.dataEndDate = datetime.now()

    # ----------------------------------------------------------------------
    def setInitCapital(self, initCapital):
        """设置初始资金"""
        self.initCapital = initCapital

    # ----------------------------------------------------------------------
    def setStrategySetting(self, setting):
        """设置策略信息和参数"""
        # 传入的策略参数包含市场、交易标的、交易频率等参数
        self.market = setting["market"]           # 定义市场
        self.symbol = setting["symbol"]           # 定义交易标的
        self.frequency = setting["frequency"]     # 定义交易频率
        self.dbName = "_".join([self.market, self.frequency])  # 锁定数据库
        # 创建策略信息字典
        self.strategyInfo = setting

    # ----------------------------------------------------------------------
    # 数据获取模块
    # ----------------------------------------------------------------------
    def loadBar(self):
        """获取K线数据(这里认为获取K线就是初始化的情况)"""
        return self.initData

    # ----------------------------------------------------------------------
    # 日志和输出模块
    # ----------------------------------------------------------------------
    def output(self, content):
        """输出内容"""
        print(str(datetime.now()) + "\t" + content)

    # ----------------------------------------------------------------------
    def get_logs_path(self):
        """获取日志保存目录"""
        logs_folder = os.path.abspath(os.path.join(os.getcwd(), 'logs'))
        # 如果不存在该文件夹则创建
        if not os.path.exists(logs_folder):
            os.mkdir(logs_folder)
        return logs_folder

    # ----------------------------------------------------------------------
    # 策略启动模块
    # ----------------------------------------------------------------------
    def initStrategy(self, strategyClass):
        """实例化策略，就是将回测引擎传给策略的过程"""
        # 整理setting，在setStrategySetting的时候得到strategyInfo信息字典
        # 此时包含策略名称、市场、标的、频率以及update的交易参数
        # 先将交易参数拿出来，再讲交易参数一个个的拼接到setting里
        setting = dict(self.strategyInfo)
        del setting["d"]
        setting.update(self.strategyInfo["d"])  # 加入策略交易参数

        """比较绕的写法，在回测引擎内部将回测引擎传给策略"""
        # 实例化策略，此处将回测引擎和setting配置传入策略里
        self.strategy = strategyClass(self, setting)
        # 创建账户实例,并初始化资金
        self.account = vo.AccountData(self.initCapital)

        # 设置策略文件夹名称
        dtNow = datetime.now()
        self.strategy_name = "{}_{}_{}".format(self.strategyInfo["ClassName"],
                                               dtNow.strftime("%Y%m%d"),
                                               dtNow.strftime("%H%M%S"))

    # ----------------------------------------------------------------------
    def runBacktesting(self, strategyClass):
        """运行策略回测，传入回测的策略类"""
        # ------------------------------------------------------------------
        # 初始化策略
        self.initStrategy(strategyClass)
        # ------------------------------------------------------------------
        # 连接数据库
        collection = self.dbClient[self.dbName][self.symbol]
        dataClass = vo.BarData
        self.output(u'开始回测')
        # ------------------------------------------------------------------
        # 读取数据并推送
        # 初始化数据的时间范围（前闭后开）
        flt = {'start_time': {'$gte': self.dataStartDate,
                              '$lt': self.strategyStartDate}}
        # 读取数据
        initCursor = collection.find(flt).sort('start_time', pymongo.ASCENDING)
        # 将bar数据以类的形式存放在data列表里面
        self.initData = []
        for d in initCursor:
            # 先将data生成一个数据类，即vo.BarData的k线数据类
            data = dataClass()
            # 利用字典属性，将数据传递进去
            data.__dict__ = d
            self.initData.append(data)
        if len(self.initData) == 0:
            self.output(u"提示：初始化数据量为0")
        # 策略执行初始化
        self.strategy.onInit()
        self.output(u'策略启动完成')
        # 读取回测数据
        flt = {'start_time': {'$gte': self.strategyStartDate,
                              '$lte': self.dataEndDate}}
        dataCursor = collection.find(flt).sort('start_time', pymongo.ASCENDING)
        dataCount = 0
        for d in dataCursor:
            data = dataClass()
            data.__dict__ = d
            """对新的K线数据进行迭代"""
            self.bar = data             # 最新K线对象
            self.dt = data.end_time     # 最新K线时间(使用K线结束时间)
            self.crossLimitOrder()      # 先撮合限价单
            self.strategy.onBar(data)   # 推送K线到策略中
            self.updateAccount()        # 更新账户信息
            self.updateDailyResult()    # 计算每日结果
            dataCount += 1
        if dataCount == 0:
            self.output(u"出错: 回测数据量为0")
        self.output(u'数据回放结束')
        # 断开数据库
        self.dbClient.close()

    # ----------------------------------------------------------------------
    # 委托单模块
    # ----------------------------------------------------------------------
    def sendOrder(self, symbol, orderType, price, volume):
        """发送限价单，主要是创建订单详情"""
        # 委托编号
        self.limitOrderCount += 1
        orderID = str(self.limitOrderCount)
        """创建委托单对象"""
        order = vo.OrderData()     # 订单数据类
        order.symbol = symbol      # 订单代码
        order.orderID = orderID    # 订单ID
        order.price = price        # 订单价格，一般是bar的收盘价
        order.volume = volume      # 订单成交量
        order.orderTime = self.dt.strftime('%Y-%m-%d %H:%M:%S')
        # 委托类型映射,添加order的direction方向，offset执行交易的类型
        if orderType == vc.ORDER_BUY:              # 买
            order.offset = vc.OFFSET_OPEN
        elif orderType == vc.ORDER_SELL:           # 卖
            order.offset = vc.OFFSET_CLOSE
        # 保存到限价单字典中
        self.workingLimitOrderDict[orderID] = order
        # 限价单字典
        self.limitOrderDict[orderID] = order
        # 返回委托代码
        return orderID

    # # ----------------------------------------------------------------------
    def cancelOrder(self, orderID):
        """撤销单个限价委托单"""
        if orderID in self.workingLimitOrderDict:
            order = self.workingLimitOrderDict[orderID]
            order.cancelTime = self.dt.strftime('%Y-%m-%d %H:%M:%S')
            del self.workingLimitOrderDict[orderID]

    # ----------------------------------------------------------------------
    def cancelAll(self):
        """全部撤单"""
        for orderID in list(self.workingLimitOrderDict.keys()):
            self.cancelOrder(orderID)

    # ----------------------------------------------------------------------
    # 撮合模块
    # ----------------------------------------------------------------------
    def crossLimitOrder(self):
        """
        1、对需要交易的以开盘价进行交易，self.workingLimitOrderDict永远拿到的是上一个bar的数据
        """
        # 当前存在的限价单循环
        for orderID, order in list(self.workingLimitOrderDict.items()):
            # order.price是上一个bar收盘价
            buyCross = order.offset == vc.OFFSET_OPEN and self.bar.open
            sellCross = order.offset == vc.OFFSET_CLOSE and self.bar.open
            # 如果发生了成交
            if buyCross or sellCross:
                # 推送成交数据
                self.tradeCount += 1  # 成交编号自增1
                tradeID = str(self.tradeCount)
                """创建成交单对象"""
                trade = vo.TradeData()
                trade.symbol = order.symbol        # 记录交易标的
                trade.tradeID = tradeID            # 记录交易编号
                trade.orderID = order.orderID      # 记录订单编号
                trade.offset = order.offset        # 记录执行交易的类型
                trade.price = self.bar.open        # 以开盘价成交
                # 更新仓位
                if buyCross:
                    self.strategy.pos += order.volume
                else:
                    self.strategy.pos -= order.volume
                if isinstance(self.strategy.pos, float):
                    self.strategy.pos = int(self.strategy.pos)
                # 更新已成交量
                trade.volume = order.volume
                # 更新成交单的时间
                trade.dt = self.bar.start_time
                trade.tradeTime = trade.dt.strftime('%Y-%m-%d %H:%M:%S')
                # 将成交单添加到成交字典和成交总表
                self.tradeDict[tradeID] = trade
                self.workingTradeList.append(trade)
                # 从总限价单列表删除该已成交orderID的限价单
                if orderID in self.workingLimitOrderDict:
                    del self.workingLimitOrderDict[orderID]

    # ----------------------------------------------------------------------
    def updateAccount(self):
        """更新账户的信息"""
        # ------------------------------------------------------------------
        tradingPnl = 0.0     # 交易总利润
        commission = 0.0     # 手续费金额
        slippage = 0.0       # 滑点金额
        for trade in self.workingTradeList:
            vol = abs(trade.volume)
            # 开仓的情况
            if trade.offset == vc.OFFSET_OPEN:
                posChange = vol
            # 平仓的情况
            elif trade.offset == vc.OFFSET_CLOSE:
                posChange = -vol
            tradingPnl += posChange * (self.bar.close - trade.price)
            commission += trade.price * vol * self.rate_commission
            slippage += vol * trade.price * self.rate_slippage
        # 持仓收益：计算上一个bar结束时持仓的部分到当前bar走完时的持仓收益
        positionPnl = self.pos_last * (self.bar.close - self.close_last)
        totalPnl = tradingPnl + positionPnl
        netPnl = totalPnl - commission - slippage
        # 更新最新的账户信息
        self.account.pos = self.strategy.pos                          # 仓位数量就是策略的交易量，持续跟踪交易标的的交易情况
        self.account.capital += netPnl                                # 总资产
        self.account.totalPnl = totalPnl                              # totalPnl = tradingPnl + positionPnl
        self.account.commission = commission                          # 此处记录的只是当次成交的手续费
        self.account.slippage = slippage                              # 此处记录的只是当次成交的滑点
        self.account.netPnl = netPnl                                  # 此处记录的只是当次成交的净利润
        self.account.tradeTimes = len(self.workingTradeList)          # 此处记录的只是当次成交的交易次数
        # ------------------------------------------------------------------
        # 更新上一根bar的数据
        self.close_last = self.bar.close
        self.pos_last = self.strategy.pos
        # 删除计算过的trade字典
        self.workingTradeList = []

    # ----------------------------------------------------------------------
    def updateDailyResult(self):
        """更新每日的资金"""
        date = self.bar.start_time.date()
        if date not in self.dailyResultDict:
            self.dailyResultDict[date] = vo.DailyResult(date)
        # 更新日线结果
        self.dailyResultDict[date].update(self.bar.close, self.account.tradeTimes,
                                          self.account.pos, self.account.totalPnl,
                                          self.account.commission, self.account.slippage,
                                          self.account.netPnl, self.account.capital)

    # ----------------------------------------------------------------------
    # 策略评价模块
    # ----------------------------------------------------------------------
    def runEvaluating(self, saving=False, plotting=False):
        """策略评价"""
        # 创建评价对象
        self.evaluator = Evaluation()
        # 设置参数
        self.evaluator.set_data(self.tradeDict, self.dailyResultDict, self.account)  # 输入回测数据
        self.evaluator.set_parameter(self.rate_commission, self.fixed_slippage, self.rate_slippage,
                                     self.fixCommission, self.bar, self.initCapital)
        # 计算交易结果
        self.evaluator.cal_trade_result()
        # 计算日线结果
        self.evaluator.cal_daily_result()
        # 保存日志的情况
        if saving:
            # 设置策略保存路径
            self.strategySavingPath = os.path.abspath(os.path.join(self.get_logs_path(), self.strategy_name))
            if not os.path.exists(self.strategySavingPath):
                os.makedirs(self.strategySavingPath)
            self.evaluator.set_path(self.strategySavingPath)
        # 打印策略评价指标
        self.evaluator.print_result()
        if saving:
            self.evaluator.export_result()
        if plotting:
            self.evaluator.plot()

    # ----------------------------------------------------------------------
    def runEvaluating_optimization(self):
        """参数优化模式下, 策略快速评价"""
        # 创建评价对象
        self.evaluator = Evaluation()
        # 设置参数
        self.evaluator.set_data(self.tradeDict, self.dailyResultDict, self.account)       # 输入回测数据
        self.evaluator.set_parameter(self.rate_commission, self.fixed_slippage, self.rate_slippage,
                                     self.fixCommission, self.bar, self.initCapital)
        # 计算日线结果
        self.evaluator.cal_daily_result()

    # ----------------------------------------------------------------------
    # 参数优化模块
    # ----------------------------------------------------------------------
    def runParallelOptimization(self, strategyClass, optimizationSetting):
        """并行优化参数"""
        # 记录开始时间
        t1 = time.time()

        # 获取优化设置
        settingList = optimizationSetting.generateSetting()
        targetName = optimizationSetting.optimizeTarget

        # 检查参数设置问题
        if not settingList or not targetName:
            self.output(u'优化设置有问题，请检查')

        # 多进程优化，启动一个对应CPU核心数量的进程池
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        print(u"CPU核数:{}".format(multiprocessing.cpu_count()))
        op_list = []
        for setting in settingList:
            op_list.append(pool.apply_async(optimize, (strategyClass, setting, targetName,
                                            self.ipAddress, self.port, self.initCapital,
                                            self.startDate, self.initDays, self.endDate,
                                            self.strategyInfo)))

        pool.close()
        pool.join()

        # 结果处理
        resultList = [res.get() for res in op_list]
        # 转化成df
        df = pd.DataFrame(resultList)
        df.sort_values(by=targetName, axis=0, ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # 保存到本地文件夹
        # 设置参数优化保存路径
        dtNow = datetime.now()
        strategyName = "Optimization_{}_{}".format(dtNow.strftime("%Y%m%d"), dtNow.strftime("%H%M%S"))
        strategySavingPath = os.path.abspath(os.path.join(self.get_logs_path(), strategyName))
        # 如果不存在该路径则创建
        if not os.path.exists(strategySavingPath):
            os.makedirs(strategySavingPath)
        # 保存参数优化结果excel
        file_df = os.path.abspath(os.path.join(strategySavingPath, "Result.xlsx"))
        df.to_excel(file_df, index=False)
        # 保存策略基本信息
        file_txt = os.path.abspath(os.path.join(strategySavingPath, "策略基本信息.txt"))
        f = open(file_txt, "w")
        f.write(u"策略名称: {}\n".format(self.strategyInfo["ClassName"]))
        f.write(u"品种: {}\n".format(self.strategyInfo["symbol"]))
        f.write(u"周期: {}\n".format(self.strategyInfo["frequency"]))
        f.write(u"开始时间: {}, 结束时间: {}\n".format(self.startDate, self.endDate))
        f.write(u"优化目标: {}\n".format(targetName))
        f.write(u"参数组合:\n")
        settingContent = u"{}".format(settingList)
        settingContent = "},\n".join(settingContent.split("},"))
        f.write(settingContent)
        f.close()

        print(df.head(10))   # 打印前几行
        print(u"参数组合个数: {}".format(len(df)))
        time_all = time.time() - t1
        time_mean = time_all / len(df)
        print(u"总耗时:{:.2f}s, 平均每组耗时:{:.2f}s".format(time_all,time_mean))


class OptimizationSetting(object):
    """优化设置"""
    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.paramDict = OrderedDict()
        self.settingList = []
        self.optimizeTarget = ''        # 优化目标字段

    # ----------------------------------------------------------------------
    def addParameter(self, name, start, end=None, step=None):
        """增加优化参数"""
        if end is None and step is None:
            self.paramDict[name] = [start]
            return
        if end < start:
            print(u'参数起始点必须不大于终止点')
            return
        if step <= 0:
            print(u'参数布进必须大于0')
            return
        _list = []
        param = start
        while param <= end:
            _list.append(param)
            param += step
        self.paramDict[name] = _list

    # ----------------------------------------------------------------------
    def generateSetting(self):
        """生成优化参数组合"""
        # 参数名的列表
        nameList = list(self.paramDict.keys())
        paramList = list(self.paramDict.values())

        # 使用迭代工具生产参数对组合
        # product创建一个迭代器，生成表示item1，item2等中的项目的笛卡尔积的元组，repeat是一个关键字参数，指定重复生成序列的次数。
        productList = list(product(*paramList))

        # 把参数对组合打包到一个个字典组成的列表中
        settingList = []
        for p in productList:
            d = dict(zip(nameList, p))
            settingList.append(d)

        return settingList

    # ----------------------------------------------------------------------
    def setOptimizeTarget(self, target):
        """设置优化目标字段"""
        self.optimizeTarget = target


def optimize(strategyClass, opSetting, targetName,
             address, port, initalCapital,
             startDate, initDays, endDate,
             setting):
    """多进程优化时跑在每个进程中运行的函数"""
    engine = BacktestingEngine()                        # 创建单个回测实例
    engine.setDatabase(address=address, port=port)      # 设置MongoDB数据库地址
    engine.setInitCapital(initalCapital)                # 设置
    engine.setStartDate(startDate, initDays)            # 策略开始日期, 往前加载多少天的数据
    engine.setEndDate(endDate)                          # 策略结束日期
    setting['d'] = opSetting                            # 添加某个策略参数
    engine.setStrategySetting(setting)                  # 设置策略信息和参数
    engine.runBacktesting(strategyClass)                # 跑回测
    engine.runEvaluating_optimization()                 # 策略评价
    # 尝试获取评价指标
    try:
        targetValue = engine.evaluator.evaluate_daily[targetName]
    except KeyError:
        targetValue = 0
    # 这里将目标指标添加到setting中,并返回
    opSetting[targetName] = targetValue
    return opSetting
