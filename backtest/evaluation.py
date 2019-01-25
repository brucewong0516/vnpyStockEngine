# -*- coding: utf-8 -*-
"""
策略评价指标模块
"""
import os
import copy
import numpy as np
import pandas as pd
from datetime import datetime
from collections import OrderedDict
import matplotlib.pyplot as plt
# 导入自定义的模块
from backtest import constant as vc  # 常量模块


class Evaluation(object):
    """评价指标对象"""
    # ----------------------------------------------------------------
    def __init__(self):
        """构造器"""
        # ------------------------------------------------------------
        # 输入的数据
        self.tradeDict = {}             # 成交对象字典
        self.dailyResultDict = {}       # 每日成交结果字典(只是初始化了时间,结果还没算)
        self.account = None             # 最终账户信息
        self.rate = 0.0                 # 手续费比例
        self.slippage = 0.0             # 滑点数值
        self.slippageRate = 0.0         # 滑点比例
        self.size = 1                   # 合约大小
        self.fixCommission = 1.0        # 固定手续费
        self.endBar = None              # 回测完毕最后1根K线
        self.initCapital = 100000       # 初始资金
        self.strategyPath = ""          # 策略保存路径
        self.evaluatorPath = ""         # 策略评价指标txt路径

        # ------------------------------------------------------------
        # 中间和结果变量
        self.tradeResultList = []       # 每笔交易结果储存列表
        self.evaluate_trade = {}        # 交易结果评价指标(胜率盈亏比等)
        self.df_daily = None            # 日线交易结果df
        self.evaluate_daily = {}        # 日线交易结果评价指标(收益风险比等)
        self.df_master = None           # 行情把握度df

    # ----------------------------------------------------------------
    def set_data(self, tradeDict, dailyResultDict, account):
        """设置输入的数据"""
        self.tradeDict = tradeDict
        self.dailyResultDict = dailyResultDict
        self.account = account

    # ----------------------------------------------------------------
    def set_parameter(self, rate, slip, slipRate, fix, bar, initCapital):
        """设置交易参数"""
        self.rate = rate                # 手续费比例
        self.slippage = slip            # 滑点数值
        self.slippageRate = slipRate    # 滑点百分比
        self.fixCommission = fix        # 固定手续费
        self.endBar = bar               # 回测完毕最后1根K线
        self.initCapital = initCapital  # 策略初始资金

    # ----------------------------------------------------------------
    def set_path(self, path):
        """设置策略保存路径"""
        self.strategyPath = path
        self.evaluatorPath = os.path.abspath(os.path.join(path, "Evaluation.txt"))

    # ----------------------------------------------------------------
    def cal_trade_result(self):
        """计算基于成交记录的评价结果"""
        # 首先基于回测后的成交记录，计算每笔交易的盈亏
        resultList = []   # 交易结果列表
        longTrade = []    # 未平仓的多头交易(动态)
        shortTrade = []   # 未平仓的空头交易(动态)
        posList = [0]     # 每笔成交后的持仓情况

        # 所有交易循环(计算交易结果对象tradeResult)
        for tid, trade in self.tradeDict.items():
            # 复制成交对象，因为下面的开平仓交易配对涉及到对成交数量的修改
            # 若不进行复制直接操作，则计算完后所有成交的数量会变成0
            trade = copy.copy(trade)

            # 多头交易
            if trade.offset == vc.OFFSET_OPEN:
                # 开多
                longTrade.append(trade)
            else:
                while True:
                    entryTrade = longTrade[0]
                    exitTrade = trade

                    # 清算开平仓交易
                    closedVolume = min(exitTrade.volume, entryTrade.volume)
                    result = TradingResult(entryTrade.price, entryTrade.dt, exitTrade.price, exitTrade.dt,
                                           closedVolume, self.rate, self.slippage, self.slippageRate, self.size,
                                           fixcommission=self.fixCommission)
                    resultList.append(result)
                    posList.extend([1, 0])

                    # 计算未清算部分
                    entryTrade.volume -= closedVolume
                    exitTrade.volume -= closedVolume
                    if isinstance(entryTrade.volume, float):
                        entryTrade.volume = round(entryTrade.volume, 3)
                    if isinstance(exitTrade.volume, float):
                        exitTrade.volume = round(exitTrade.volume, 3)

                    # 如果开仓交易已经全部清算，则从列表中移除
                    if not entryTrade.volume:
                        longTrade.pop(0)

                    # 如果平仓交易已经全部清算，则退出循环
                    if not exitTrade.volume:
                        break

                    # 如果平仓交易未全部清算，
                    if exitTrade.volume:
                        # 且开仓交易已经全部清算完，则平仓交易剩余的部分
                        # 等于新的反向开仓交易，添加到队列中
                        if not longTrade:
                            shortTrade.append(exitTrade)
                            break
                        # 如果开仓交易还有剩余，则进入下一轮循环
                        else:
                            pass

        # 到最后交易日尚未平仓的交易，则以最后价格平仓
        endPrice = self.endBar.close

        # 多单循环平仓
        for trade in longTrade:
            result = TradingResult(trade.price, trade.dt, endPrice, self.endBar.end_time, trade.volume,
                                   self.rate, self.slippage, self.slippageRate, self.size,
                                   fixcommission=self.fixCommission)
            resultList.append(result)

        # 检查是否有交易
        if not resultList:
            print(u"出错: 无交易结果--resultDict")
            return {}

        # 然后基于每笔交易的结果，我们可以计算具体的盈亏曲线和最大回撤等
        # 主要是为了计算 胜率,盈亏比,总金额和总手续费等
        pnlList = []                # 净利润序列
        capital = self.initCapital  # 总资产(动态)
        capitalList = []            # 总资产序列

        totalResult = 0             # 总交易次数(一开一平)
        totalTurnover = 0           # 总成交金额（合约面值）
        totalCommission = 0         # 总手续费
        totalSlippage = 0           # 总滑点

        winningResult = 0           # 盈利次数
        losingResult = 0            # 亏损次数
        totalWinning = 0            # 总盈利金额
        totalLosing = 0             # 总亏损金额

        winningTimes = 0            # 连赢次数
        losingTimes = 0             # 连亏次数
        winningTimesList = []       # 连赢次数序列
        losingTimesList = []        # 连亏次数序列

        # 所有交易结果循环
        for result in resultList:
            totalResult += 1
            capital += result.pnl
            totalTurnover += result.turnover
            totalCommission += result.commission
            totalSlippage += result.slippage

            if result.pnl >= 0:
                winningResult += 1
                totalWinning += result.pnl
                winningTimes += 1
                losingTimes = 0
            else:
                losingResult += 1
                totalLosing += result.pnl
                losingTimes += 1
                winningTimes = 0

            winningTimesList.append(winningTimes)
            losingTimesList.append(losingTimes)
            pnlList.append(result.pnl)
            capitalList.append(capital)

        # 计算盈亏相关数据
        # (1) 计算胜率
        winningRate = winningResult / totalResult              # 胜率
        # (2) 计算盈亏比
        averageWinning = 0  # 这里把数据都初始化为0
        averageLosing = 0
        profitLossRatio = 0
        if winningResult:
            averageWinning = totalWinning / winningResult      # 平均每笔盈利
        if losingResult:
            averageLosing = totalLosing / losingResult         # 平均每笔亏损
        if averageLosing:
            profitLossRatio = -averageWinning / averageLosing  # 盈亏比
        # (3) 计算总盈亏
        if totalLosing:
            totalWinLoss = totalWinning / abs(totalLosing)
        else:
            totalWinLoss = 0

        # 储存评价指标
        d = dict()
        d['tradeTimes'] = totalResult             # 总交易次数
        d['winRate'] = winningRate                # 胜率
        d['profitLossRatio'] = profitLossRatio    # 盈亏比
        d['totalWinLoss'] = totalWinLoss          # 总盈亏
        d['averageWinning'] = averageWinning      # 平均每笔盈利
        d['averageLosing'] = averageLosing        # 平均每笔亏损
        d['totalTurnover'] = totalTurnover        # 总交易金额(合约面值)
        d['totalCommission'] = totalCommission    # 总手续费
        d['totalSlippage'] = totalSlippage        # 总滑点金额
        d['maxWinTimes'] = max(winningTimesList)  # 最大连赢次数
        d['maxLossTimes'] = max(losingTimesList)  # 最大连亏次数
        d['pnlList'] = pnlList                    # 净利润序列
        d['capitalList'] = capitalList            # 总资产序列

        self.tradeResultList = resultList
        self.evaluate_trade = d

    # ----------------------------------------------------------------
    def cal_daily_result(self):
        """计算基于日线的评价指标"""
        dailyResult_sample = list(self.dailyResultDict.values())[0]
        resultDict = {k: [] for k in dailyResult_sample.__dict__.keys()}
        for dailyResult in self.dailyResultDict.values():
            for k, v in dailyResult.__dict__.items():
                resultDict[k].append(v)
        df = pd.DataFrame.from_dict(resultDict)

        # 计算衍生数据
        df = df.set_index('date')
        df['balance'] = df['netPnl'].cumsum() + self.initCapital       # 计算总资产每日序列
        df['return'] = (np.log(df['balance']) - np.log(df['balance'].shift(1))).fillna(0)  # 计算每日收益率序列
        df['highlevel'] = df['balance'].rolling(min_periods=1, window=len(df),
                                                center=False).max()    # 计算每日总资产最大值序列
        df['drawdown'] = df['balance'] - df['highlevel']               # 计算每日最大回撤序列
        df['drawdownR'] = df['drawdown'] / df['highlevel']             # 计算每日最大回撤比例序列

        # 计算统计结果
        startDate = df.index[0]                                    # 回测开始日期,date格式
        endDate = df.index[-1]                                     # 回测结束日期,date格式

        totalDays = len(df)                                        # 交易日数量
        profitDays = len(df[df['netPnl'] > 0])                     # 盈利交易日数量
        lossDays = len(df[df['netPnl'] < 0])                       # 亏损交易日数量

        endBalance = df['balance'].iloc[-1]                        # 最终总资产
        maxBalance = df['balance'].max()                           # 总资产最大值
        maxDf = df[df["balance"] == maxBalance]
        maxBalanceDate = maxDf.index.tolist()[0]                   # 总资产最大值的日期

        maxDrawdown = df['drawdown'].min()                         # 最大回撤金额
        maxDrawdownRate = df['drawdownR'].min()                    # 最大回撤比例

        totalPnl = df['totalPnl'].sum()                            # 利润总额(不考虑手续费和滑点)
        totalCommission = df['commission'].sum()                   # 手续费总额
        totalSlippage = df['slippage'].sum()                       # 滑点总额
        totalNetPnl = df['netPnl'].sum()                           # 净利润总额
        dailyNetPnl = totalNetPnl / totalDays                      # 平均每个交易日净利润

        totalReturn = endBalance / self.initCapital - 1            # 总利润率
        dailyReturn = df['return'].mean()                          # 日收益率均值
        returnStd = df['return'].std()                             # 日收益率标准差
        noRiskRate = 0.06                                          # 无风险利率, 这里设为6%
        noRiskRateDaily = pow((noRiskRate + 1), 1.0 / 240) - 1     # 无风险利率(日)

        # 计算夏普比例
        if returnStd:
            sharpeRatio = (dailyReturn - noRiskRateDaily) / returnStd * np.sqrt(240)
        else:
            sharpeRatio = 0

        # 计算回测时长(年)
        time_delta = endDate - startDate
        year_seconds = time_delta.total_seconds()
        years = year_seconds/(60*60*24*365)
        # years = totalDays / 250  # 回测的年份

        # 计算收益率,最大回撤比例,收益风险比
        yearReturn = pow((totalReturn + 1), 1.0 / years) - 1  # 年化收益率(复利)
        yearReturn2 = totalReturn / years                     # 年化收益率(简单)
        if abs(maxDrawdownRate) == 0:
            returnRisk = 0
            returnRisk2 = 0
        else:
            returnRisk = yearReturn / abs(maxDrawdownRate)    # 收益风险比(复利)
            returnRisk2 = yearReturn2 / abs(maxDrawdownRate)  # 收益风险比(简单)

        # 返回结果
        d = dict()
        # 时间相关
        d['startDate'] = startDate      # 开始日期,date
        d['endDate'] = endDate          # 结束日期,date
        d['totalDays'] = totalDays      # 总交易日,int
        d['years'] = years              # 年份,float
        d['profitDays'] = profitDays    # 盈利交易日数量
        d['lossDays'] = lossDays        # 亏损交易日数量
        # 利润成本相关
        d['totalPnl'] = totalPnl                # 利润总额
        d['totalCommission'] = totalCommission  # 手续费总额
        d['totalSlippage'] = totalSlippage      # 滑点总额
        d['totalNetPnl'] = totalNetPnl          # 净利润总额
        d['dailyNetPnl'] = dailyNetPnl          # 每日平均净利润
        # 收益风险相关
        d['endBalance'] = endBalance            # 期末总资产
        d['maxBalance'] = maxBalance            # 总资产最大值
        d['maxBalanceDate'] = maxBalanceDate    # 总资产最大值的日期
        d['maxDrawdown'] = maxDrawdown          # 最大回撤金额
        d['maxDrawdownRate'] = maxDrawdownRate  # 最大回撤比例
        d['totalReturn'] = totalReturn      # 总收益率
        d['yearReturn'] = yearReturn        # 年化收益率(复利)
        d['yearReturn2'] = yearReturn2      # 年化收益率(简单)
        d['returnRisk'] = returnRisk        # 收益风险比(复利)
        d['returnRisk2'] = returnRisk2      # 收益风险比(简单)
        d['sharpeRatio'] = sharpeRatio      # 夏普比例

        # 赋值
        self.df_daily = df             # 日线交易结果df
        self.evaluate_daily = d        # 日线交易结果评价指标(收益风险比等)

    # ----------------------------------------------------------------
    def __print_save(self,content):
        """内置函数, 打印和保存"""
        dt = datetime.now()
        content_dt = "{}  {}".format(dt, content)
        print(content_dt)
        # 是否要保存
        if self.evaluatorPath:
            f = open(self.evaluatorPath, "a")
            f.write(content)
            f.write("\n")
            f.close()

    # ----------------------------------------------------------------
    def print_result(self):
        """打印回测结果"""

        # 时间相关
        self.__print_save(u"-----------时间相关-------------")
        self.__print_save(u"策略开始日期: {}".format(self.evaluate_daily["startDate"]))
        self.__print_save(u"策略结束日期: {}".format(self.evaluate_daily["endDate"]))
        totalDays = self.evaluate_daily["totalDays"]
        self.__print_save(u"交易日总数量: {}".format(totalDays))
        self.__print_save(u"盈利交易日数量: {}, 占比: {:.1f}%".format(self.evaluate_daily["profitDays"],
                                                                self.evaluate_daily["profitDays"] / totalDays * 100))
        self.__print_save(u"亏损交易日数量: {}, 占比: {:.1f}%".format(self.evaluate_daily["lossDays"],
                                                                self.evaluate_daily["lossDays"] / totalDays * 100))
        self.__print_save(u"交易日总数量: {}".format(self.evaluate_daily["totalDays"]))
        self.__print_save(u"年份: {:.1f}".format(self.evaluate_daily["years"]))

        # 交易相关
        self.__print_save(u"")
        self.__print_save(u"-----------交易相关-------------")
        tradeTimes = self.evaluate_trade["tradeTimes"]
        self.__print_save(u"总交易次数: {:.0f}".format(tradeTimes))
        self.__print_save(u"年均交易次数: {:.0f}".format(tradeTimes / self.evaluate_daily["years"]))
        self.__print_save(u"月均交易次数: {:.0f}".format(tradeTimes / (self.evaluate_daily["years"] * 12)))
        self.__print_save(u"胜率: {:.2f}%".format(self.evaluate_trade["winRate"] * 100))
        self.__print_save(u"盈亏比: {:.2f}".format(self.evaluate_trade["profitLossRatio"]))
        self.__print_save(u"总盈亏: {:.2f}".format(self.evaluate_trade["totalWinLoss"]))

        # 成本相关
        self.__print_save(u"")
        self.__print_save(u"-----------成本相关-------------")
        totalPnl = self.evaluate_daily["totalPnl"]
        self.__print_save(u"总利润总额: {:.2f}, 占比: {:.1f}%".format(totalPnl, totalPnl / totalPnl * 100))
        self.__print_save(u"净利润总额: {:.2f}, 占比: {:.1f}%".format(self.evaluate_daily["totalNetPnl"],
                                                                      self.evaluate_daily["totalNetPnl"] / totalPnl * 100))
        self.__print_save(u"手续费总额: {:.2f}, 占比: {:.1f}%".format(self.evaluate_daily["totalCommission"],
                                                                      self.evaluate_daily["totalCommission"] / totalPnl * 100))
        self.__print_save(u"滑点总额: {:.2f}, 占比: {:.1f}%".format(self.evaluate_daily["totalSlippage"],
                                                                   self.evaluate_daily["totalSlippage"] / totalPnl * 100))
        # 收益相关
        self.__print_save(u"")
        self.__print_save(u"-----------收益相关-------------")
        self.__print_save(u"期初总资产: {:.2f}".format(self.initCapital))
        self.__print_save(u"期末总资产: {:.2f}".format(self.evaluate_daily["endBalance"]))
        self.__print_save(u"总收益率: {:.2f}%".format(self.evaluate_daily["totalReturn"] * 100))
        self.__print_save(u"年化收益率: {:.2f}%".format(self.evaluate_daily["yearReturn"] * 100))

        # 风险相关
        self.__print_save(u"")
        self.__print_save(u"-----------风险相关-------------")
        self.__print_save(u"总资产最大值: {:.2f}".format(self.evaluate_daily["maxBalance"]))
        self.__print_save(u"总资产最大值的日期: {}".format(self.evaluate_daily["maxBalanceDate"]))
        self.__print_save(u"最大回撤金额: {:.2f}".format(self.evaluate_daily["maxDrawdown"]))
        self.__print_save(u"最大回撤比例: {:.2f}%".format(self.evaluate_daily["maxDrawdownRate"] * 100))

        # 综合评价
        self.__print_save(u"")
        self.__print_save(u"-----------综合评价-------------")
        self.__print_save(u"收益风险比: {:.2f}".format(self.evaluate_daily["returnRisk"]))
        self.__print_save(u"夏普比率: {:.2f}".format(self.evaluate_daily["sharpeRatio"]))

    # ----------------------------------------------------------------
    def export_result(self):
        """导出成交记录和日线交易结果"""
        if not self.strategyPath:
            print(u"策略保存路径没有设置,无法保存交易结果")
            return
        # 合成成交记录df
        tradeDictList = []
        tradeID = 0
        for result in self.tradeResultList:
            tradeID += 1
            row = OrderedDict()
            row[u"编号"] = tradeID
            row[u"开仓时间"] = result.entryDt
            row[u"平仓时间"] = result.exitDt
            row[u"开仓价格"] = result.entryPrice
            row[u"平仓价格"] = result.exitPrice
            row[u"数量"] = result.volume
            row[u"总利润"] = result.totalProfit
            row[u"手续费"] = result.commission
            row[u"滑点费用"] = result.slippage
            row[u"净利润"] = result.pnl
            tradeDictList.append(row)
        df_tradeRecord = pd.DataFrame(tradeDictList)
        # 保存成交记录
        tradeRecordName = "{}.csv".format("TradeRecord")
        tradeRecordPath = os.path.abspath(os.path.join(self.strategyPath, tradeRecordName))
        df_tradeRecord.to_csv(tradeRecordPath, index=False, encoding="gbk")

        # 导出每日净值csv
        dfDailyPath = os.path.abspath(os.path.join(self.strategyPath, "df_daily.csv"))
        dfDaily = copy.deepcopy(self.df_daily)
        if "tradeList" in dfDaily.columns.tolist():
            dfDaily.drop(["tradeList"], axis=1, inplace=True)
        dfDaily.to_csv(dfDailyPath, index=True, encoding="gbk")

    # ----------------------------------------------------------------
    def plot(self):
        """画图"""
        try:
            import seaborn as sns  # 如果安装了seaborn则设置为白色风格
            sns.set_style('whitegrid')
        except ImportError:
            pass
        # 画出资产曲线和最大回撤图
        f2 = plt.figure(figsize=(10, 16))
        # 日线总资产
        pBalance = plt.subplot(2, 1, 1)
        pBalance.set_title('Daily Capital')
        self.df_daily['balance'].plot()          # legend=True
        # 最大回撤
        pDrawdown = plt.subplot(2, 1, 2)
        pDrawdown.set_title('Drawdown')
        x1 = list(self.df_daily.index)            # 日期dt列表
        y1 = np.array(self.df_daily['drawdown'])  # 最大回撤序列
        pDrawdown.fill_between(x1, y1)
        plt.show()

        # 画出资产曲线和价格走势对比图
        f3 = plt.figure()
        ax1 = f3.add_subplot(111)
        y1 = np.array(self.df_daily['balance'])     # 日净值序列
        y2 = np.array(self.df_daily['closePrice'])  # 日收盘价序列
        x1 = list(self.df_daily.index)              # 日期dt列表
        ax1.plot(x1, y1, 'r-', label="Capital", linewidth=2)
        ax1.grid(False)
        ax2 = ax1.twinx()
        ax2.plot(x1, y2, 'b-', label="Price", linewidth=1)
        ax1.legend(loc='upper right')
        ax2.legend(loc='upper left')
        plt.show()

        # 保存图片
        if self.strategyPath:
            f2Path = os.path.abspath(os.path.join(self.strategyPath, "figure2.png"))
            f3Path = os.path.abspath(os.path.join(self.strategyPath, "figure3.png"))
            f2.savefig(f2Path)
            f3.savefig(f3Path)


class TradingResult(object):
    """每笔交易的结果"""
    # ----------------------------------------------------------------------
    def __init__(self, entryPrice, entryDt, exitPrice, exitDt, volume,
                 rate, slippage, slippageRate, size, fixcommission=vc.EMPTY_FLOAT):
        """Constructor"""
        self.entryPrice = entryPrice   # 开仓价格
        self.exitPrice = exitPrice     # 平仓价格

        self.entryDt = entryDt         # 开仓时间datetime
        self.exitDt = exitDt           # 平仓时间

        self.volume = volume           # 交易数量（+/-代表方向）

        self.turnover = (self.entryPrice + self.exitPrice) * size * abs(volume)  # 成交金额
        if fixcommission:
            self.commission = fixcommission * abs(self.volume)
        else:
            self.commission = abs(self.turnover * rate)                          # 手续费成本
        # 使用百分比计算滑点的情况
        if slippageRate != 0.0:
            self.slippage = (entryPrice + exitPrice) * slippageRate * size * abs(volume)
        # 使用固定金额计算滑点的情况
        else:
            self.slippage = slippage * 2 * size * abs(volume)
        self.totalProfit = (self.exitPrice - self.entryPrice) * volume * size  # 总利润
        self.pnl = (self.totalProfit - self.commission - self.slippage)        # 净盈亏
