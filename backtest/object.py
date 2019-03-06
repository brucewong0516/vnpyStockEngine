# -*- coding: utf-8 -*-
"""
定义了回测需要的所有类
"""
import talib
import numpy as np
from collections import defaultdict
from backtest.constant import (EMPTY_STRING, EMPTY_UNICODE, EMPTY_FLOAT, EMPTY_INT)


class BarData(object):
    """K线数据"""

    # ----------------------------------------------------------------------
    def __init__(self):
        self.exchange = EMPTY_STRING             # 交易所代码
        self.symbol = EMPTY_STRING               # 交易所_品种代码, 全部小写
        self.start_time = None                   # K线开始的时间, datetime对象
        self.end_time = None                     # K线结束的时间, datetime对象
        self.date = EMPTY_STRING                 # bar开始的时间，日期
        self.time = EMPTY_STRING                 # 时间
        self.frequency = EMPTY_STRING            # K线的频率, 可选的有"1min","3min","5min","15min","30min",
        self.open = EMPTY_FLOAT                  # 开盘价
        self.high = EMPTY_FLOAT                  # 最高价
        self.low = EMPTY_FLOAT                   # 最低价
        self.close = EMPTY_FLOAT                 # 收盘价
        self.volume = EMPTY_FLOAT                # 成交量(交易品种的数量)
        self.amount = EMPTY_FLOAT                # 成交额(基础品种的数量)


class TickData(object):
    """tick数据"""

    def __init__(self):
        self.symbol = EMPTY_STRING
        self.date = None
        self.last_close = EMPTY_FLOAT
        self.open = EMPTY_FLOAT
        self.high = EMPTY_FLOAT
        self.low = EMPTY_FLOAT
        self.close = EMPTY_FLOAT
        self.volume = EMPTY_FLOAT
        self.money = EMPTY_FLOAT
        self.tradedCount = EMPTY_FLOAT
        self.totalDeputeBuy = EMPTY_FLOAT
        self.averageBuy = EMPTY_FLOAT
        self.totalDeputeSell = EMPTY_FLOAT
        self.averageSell = EMPTY_FLOAT
        self.sellP1 = EMPTY_FLOAT
        self.sellP2 = EMPTY_FLOAT
        self.sellP3 = EMPTY_FLOAT
        self.sellP4 = EMPTY_FLOAT
        self.sellP5 = EMPTY_FLOAT
        self.sellP6 = EMPTY_FLOAT
        self.sellP7 = EMPTY_FLOAT
        self.sellP8 = EMPTY_FLOAT
        self.sellP9 = EMPTY_FLOAT
        self.sellP10 = EMPTY_FLOAT
        self.sellV1 = EMPTY_FLOAT
        self.sellV2 = EMPTY_FLOAT
        self.sellV3 = EMPTY_FLOAT
        self.sellV4 = EMPTY_FLOAT
        self.sellV5 = EMPTY_FLOAT
        self.sellV6 = EMPTY_FLOAT
        self.sellV7 = EMPTY_FLOAT
        self.sellV8 = EMPTY_FLOAT
        self.sellV9 = EMPTY_FLOAT
        self.sellV10 = EMPTY_FLOAT
        self.sellD1 = EMPTY_FLOAT
        self.sellD2 = EMPTY_FLOAT
        self.sellD3 = EMPTY_FLOAT
        self.sellD4 = EMPTY_FLOAT
        self.sellD5 = EMPTY_FLOAT
        self.sellD6 = EMPTY_FLOAT
        self.sellD7 = EMPTY_FLOAT
        self.sellD8 = EMPTY_FLOAT
        self.sellD9 = EMPTY_FLOAT
        self.sellD10 = EMPTY_FLOAT
        self.askP1 = EMPTY_FLOAT
        self.askP2 = EMPTY_FLOAT
        self.askP3 = EMPTY_FLOAT
        self.askP4 = EMPTY_FLOAT
        self.askP5 = EMPTY_FLOAT
        self.askP6 = EMPTY_FLOAT
        self.askP7 = EMPTY_FLOAT
        self.askP8 = EMPTY_FLOAT
        self.askP9 = EMPTY_FLOAT
        self.askP10 = EMPTY_FLOAT
        self.askV1 = EMPTY_FLOAT
        self.askV2 = EMPTY_FLOAT
        self.askV3 = EMPTY_FLOAT
        self.askV4 = EMPTY_FLOAT
        self.askV5 = EMPTY_FLOAT
        self.askV6 = EMPTY_FLOAT
        self.askV7 = EMPTY_FLOAT
        self.askV8 = EMPTY_FLOAT
        self.askV9 = EMPTY_FLOAT
        self.askV10 = EMPTY_FLOAT
        self.askD1 = EMPTY_FLOAT
        self.askD2 = EMPTY_FLOAT
        self.askD3 = EMPTY_FLOAT
        self.askD4 = EMPTY_FLOAT
        self.askD5 = EMPTY_FLOAT
        self.askD6 = EMPTY_FLOAT
        self.askD7 = EMPTY_FLOAT
        self.askD8 = EMPTY_FLOAT
        self.askD9 = EMPTY_FLOAT
        self.askD10 = EMPTY_FLOAT


class TradeData(object):
    """成交数据类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        self.symbol = EMPTY_STRING                # 合约代码
        self.tradeID = EMPTY_STRING               # 成交编号
        self.orderID = EMPTY_STRING               # 订单编号
        self.offset = EMPTY_UNICODE               # 成交开平仓
        self.price = EMPTY_FLOAT                  # 成交价格
        self.volume = EMPTY_FLOAT                 # 成交数量
        self.tradeTime = EMPTY_STRING             # 成交时间


class OrderData(object):
    """订单数据类"""

    # ----------------------------------------------------------------------
    def __init__(self):
        self.symbol = EMPTY_STRING  # 合约代码
        self.orderID = EMPTY_STRING  # 订单编号
        self.offset = EMPTY_UNICODE  # 报单开平仓, u"开仓"
        self.price = EMPTY_FLOAT  # 报单价格
        self.volume = EMPTY_FLOAT  # 报单数量
        self.orderTime = EMPTY_STRING  # 发单时间, "2018-10-10 19:00:00"


class AccountData(object):
    """账户数据类"""

    # ------------------------------------------------------------------
    def __init__(self, initCapital=1000000):
        self.date = None                      # 最新的时间, end_time
        self.bar = None                       # 最新的K线
        self.cash = initCapital               # 最新现金值
        self.deposit = EMPTY_FLOAT            # 保证金金额
        self.pos = EMPTY_INT                  # 最新持仓数量
        self.capital = initCapital            # 最新总资产
        self.totalPnl = EMPTY_FLOAT           # 总利润(当前K线的)
        self.commission = EMPTY_FLOAT         # 手续费(当前K线的)
        self.slippage = EMPTY_FLOAT           # 滑点金额(当前K线的)
        self.netPnl = EMPTY_FLOAT             # 净利润(当前K线的)
        self.tradeTimes = EMPTY_FLOAT         # 交易次数(当前K线的)
        self.available = EMPTY_FLOAT          # 可用资金(当前K线的)


class MultiAccountData(object):
    """账户数据类"""

    # ------------------------------------------------------------------
    def __init__(self, initCapital=1000000):
        self.date = None                      # 最新的时间
        self.pos = None                       # 最新持仓数量
        self.capital = initCapital            # 最新总资产
        self.totalPnl = EMPTY_FLOAT           # 总利润(当前tick)
        self.commission = EMPTY_FLOAT         # 手续费(当前tick)
        self.slippage = EMPTY_FLOAT           # 滑点金额(当前tick)
        self.netPnl = EMPTY_FLOAT             # 净利润(当前tick)
        self.available = initCapital          # 可用资金(当前tick)


class SubAccountData(object):
    """账户数据类"""

    # ------------------------------------------------------------------
    def __init__(self):
        self.date = None                             # 最新的时间, end_time
        self.bar = None                              # 最新的K线
        self.pos = EMPTY_INT                         # 最新持仓数量
        self.totalPnl = EMPTY_FLOAT                  # 总利润(当前tick)
        self.commission = EMPTY_FLOAT                # 手续费(当前tick)
        self.slippage = EMPTY_FLOAT                  # 滑点金额(当前tick)
        self.netPnl = EMPTY_FLOAT                    # 净利润(当前tick)


class DailyResult(object):
    """日线结果对象"""

    # ----------------------------------------------------------------------
    def __init__(self, date):
        """初始化"""
        self.date = date  # 日期, date对象
        self.closePrice = 0.0  # 当日收盘价
        self.tradeCount = 0  # 交易次数
        self.closePosition = 0  # 收盘时的持仓
        self.totalPnl = 0  # 总盈亏
        self.commission = 0  # 手续费
        self.slippage = 0  # 滑点金额
        self.netPnl = 0  # 净盈亏
        self.balance = 0  # 总资产

    # ----------------------------------------------------------------------
    def update(self, price, times, pos, totalPnl, commission, slippage, netPnl, balance):
        self.closePrice = price  # 当日收盘价
        self.tradeCount += times  # 累计成交次数
        self.closePosition = pos  # 收盘时的持仓
        self.totalPnl += totalPnl  # 总盈亏
        self.commission += commission  # 累计手续费
        self.slippage += slippage  # 累计滑点金额
        self.netPnl += netPnl  # 累计净盈亏
        self.balance = balance  # 总资产


class DailyMultiResult(object):
    """日线结果对象"""

    # ----------------------------------------------------------------------
    def __init__(self, date):
        """初始化"""
        self.date = date              # 日期, date对象
        self.subClosePosition = {}    # 子账户收盘时的持仓
        self.subTotalPnl = {}         # 子账户总盈亏
        self.subCommission = {}       # 子账户手续费
        self.subSlippage = {}         # 子账户滑点金额
        self.subNetPnl = {}           # 子账户净盈亏

    # ----------------------------------------------------------------------
    def update(self, pos, subTotalPnl, subCommission, subSlippage, subNetPnl):
        self.subClosePosition = pos                    # 子账户收盘时的持仓
        self.subTotalPnl = subTotalPnl                 # 子账户总盈亏
        self.subCommission = subCommission             # 子账户手续费
        self.subSlippage = subSlippage                 # 子账户滑点金额
        self.subNetPnl = subNetPnl                     # 子账户净盈亏


class BarManager(object):
    """
    K线合成器，支持：n分钟K线合成Xn分钟K线
    """

    # ----------------------------------------------------------------------
    def __init__(self, aimFreq="1day", onUpperBar=None):
        """
        构造器, 输入的参数有:
        (1) aimFreq: 要合成的K线周期, 包括:"5min","30min","1hour","1day","1week","1month"等
        (2) onUpperBar: 大级别K线的推送函数(在策略中定义好, 在这个实例中被调用)
        """
        self.bar = None  # lowerFreq的K线对象
        self.last_bar = None  # 上一个lowerFreq的K线对象
        self.upperBar = None  # 大级别K线对象
        self.aimFreq = aimFreq  # 目标频率
        self.onUpperBar = onUpperBar  # 大级别K线的回调函数
        # K线周期分类
        if self.aimFreq.endswith("min"):
            self.type = "min"
            self.type_num = int(''.join(x for x in self.aimFreq if x.isdigit()))
        elif self.aimFreq.endswith("day"):
            self.type = "day"
            self.type_num = 1
        elif self.aimFreq.endswith("week"):
            self.type = "week"
            self.type_num = 1
        elif self.aimFreq.endswith("month"):
            self.type = "month"
            self.type_num = 1

    # ----------------------------------------------------------------------
    def updateBar(self, bar):
        """x1分钟K线更新"""
        # 尚未创建对象
        if not self.upperBar:
            self.upperBar = BarData()
            self.upperBar.symbol = bar.symbol
            self.upperBar.open = bar.open
            self.upperBar.high = bar.high
            self.upperBar.low = bar.low
            self.upperBar.start_time = bar.start_time  # 开始时间戳
        # 累加老K线
        else:
            self.upperBar.high = max(self.upperBar.high, bar.high)
            self.upperBar.low = min(self.upperBar.low, bar.low)
        # 通用部分
        self.upperBar.close = bar.close
        self.upperBar.volume += float(bar.volume)
        # 小级别走完一次大级别判断
        is_end = False
        dt_end = bar.end_time  # K线结束的时间戳,为整数时间点
        # 分钟线的情况
        if self.type == "min":
            is_end = not dt_end.minute % self.type_num
        elif self.type == "day":
            is_end = dt_end.hour == 15 and dt_end.minute == 0
        elif self.type == "week":
            is_end = dt_end.hour == 0 and dt_end.minute == 0 and dt_end.weekday() == 0
        elif self.type == "month":
            a1 = dt_end.hour == 0 and dt_end.minute == 0
            a2 = dt_end.month != bar.start_time.month
            is_end = a1 and a2
        # 小级别走完一次大级别
        if is_end:
            # 生成上一大级别K线的时间戳
            self.upperBar.end_time = bar.end_time
            # 推送
            self.onUpperBar(self.upperBar)
            # 清空老K线缓存对象
            self.upperBar = None


class ArrayManager(object):
    """
    K线序列管理工具，负责：
    1. K线时间序列的维护
    2. 常用技术指标的计算
    """

    # ----------------------------------------------------------------------
    def __init__(self, size=100):
        """Constructor"""
        self.count = 0  # 缓存计数
        self.size = size  # 缓存大小
        self.inited = False  # k线容器的开关
        self.openArray = np.zeros(size)
        self.highArray = np.zeros(size)
        self.lowArray = np.zeros(size)
        self.closeArray = np.zeros(size)
        self.volumeArray = np.zeros(size)

    # ----------------------------------------------------------------------
    def updateBar(self, bar):
        """更新K线"""
        self.count += 1
        # k线容器的开关，当第一次将初始的0值全部更新为k线数据，即self.count>= self.size时，容器初始化完成
        # 不断将最新的bar数据添加进来，丢掉最久远的bar数据；
        if not self.inited and self.count >= self.size:
            self.inited = True

        self.openArray = np.append(self.openArray, bar.open)[1:]
        self.highArray = np.append(self.highArray, bar.high)[1:]
        self.lowArray = np.append(self.lowArray, bar.low)[1:]
        self.closeArray = np.append(self.closeArray, bar.close)[1:]
        self.volumeArray = np.append(self.volumeArray, bar.volume)[1:]

    # ----------------------------------------------------------------------
    @property
    def open(self):
        """获取开盘价序列"""
        return self.openArray

    # ----------------------------------------------------------------------
    @property
    def high(self):
        """获取最高价序列"""
        return self.highArray

    # ----------------------------------------------------------------------
    @property
    def low(self):
        """获取最低价序列"""
        return self.lowArray

    # ----------------------------------------------------------------------
    @property
    def close(self):
        """获取收盘价序列"""
        return self.closeArray

    # ----------------------------------------------------------------------
    @property
    def volume(self):
        """获取成交量序列"""
        return self.volumeArray

    # ------------------------------------------------------------------------
    def rolling_window(self, a, window):
        """numpy rolling"""
        shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
        strides = a.strides + (a.strides[-1],)
        return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)
