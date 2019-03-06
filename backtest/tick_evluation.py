# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
# 导入自定义的模块
from backtest.tick_engine import TickEngine


class Evaluation(TickEngine):
    # ----------------------------------------------------------------

    def __init__(self):
        super(Evaluation, self).__init__()

        # ------------------------------------------------------------
        self.accountPath = ''           # 账户信息路径
        self.strategyPath = ""          # 策略保存路径
        self.evaluatorPath = ""         # 策略评价指标txt路径
        self.tradeEvaluate = {}         # 存放最终基于成交计算的评估结果
        self.dailyEvaluate = {}         # 存放最终基于日数据计算的评估结果
        self.show_df = None             # 清洗后的日数据

    # ----------------------------------------------------------------
    def set_path(self, path):
        """设置策略保存路径"""
        self.strategyPath = path
        self.evaluatorPath = os.path.abspath(os.path.join(path, "evaluation.txt"))

    # ----------------------------------------------------------------
    def set_account_path(self, path):
        """设置策略保存路径"""
        self.accountPath = path

    # ----------------------------------------------------------------
    def cal_t_result(self):
        """计算基于剩余资金的评价结果"""
        _df = pd.read_csv(self.accountPath, index_col=0)
        _df['available_df'] = _df['available'].diff()
        _df.fillna(0, inplace=True)
        # 计算胜率
        winningRate = len(_df[_df['available_df'] > 0]) / (len(_df)-1)
        # 计算盈亏比
        averageWinning = round(_df[_df['available_df'] > 0]['available_df'].mean(), 2)
        averageLosing = round(_df[_df['available_df'] < 0]['available_df'].mean(), 2)
        if not np.isnan(averageLosing):
            profitLossRatio = -averageWinning / averageLosing  # 盈亏比
        else:
            profitLossRatio = 1
        d = dict()
        d['winRate'] = winningRate                           # 胜率
        d['profitLossRatio'] = profitLossRatio               # 盈亏比
        d['averageWinning'] = averageWinning                 # 平均每笔盈利
        d['averageLosing'] = averageLosing                   # 平均每笔亏损
        d['pnlList'] = _df['available_df'].values            # 净利润序列
        d['capitalList'] = _df['available'].values / _df['available'][0]
        self.tradeEvaluate = d

    # ----------------------------------------------------------------
    def cal_daily_result(self, ):
        """计算基于日线的评价指标"""
        df = pd.read_csv(self.accountPath, index_col=0)
        df = df[['available', 'capital', 'date', 'netPnl']]
        df['capital_pct'] = df['capital'].pct_change()
        initial_return = df['capital'][0] / self.initial_cash - 1
        df['capital_pct'].fillna(initial_return, inplace=True)
        df['cumprod_pct'] = df['capital'] / self.initial_cash
        df['cumprod_diff'] = df['capital'] - self.initial_cash
        df['cumprod_available_pct'] = df['available'] / df['available'][0]
        df['cumprod_available_diff'] = df['available'] - df['available'][0]
        # 计算衍生数据
        df = df.set_index('date')
        df['highlevel'] = df['capital'].cummax()                   # 计算每日总资产最大值序列
        df['drawdown'] = df['capital'] - df['highlevel']           # 计算每日最大回撤序列
        df['drawdownR'] = df['drawdown'] / df['highlevel']         # 计算每日最大回撤比例序列
        self.show_df = df
        # 计算统计结果
        startDate = df.index[0]                                    # 回测开始日期,date格式
        endDate = df.index[-1]                                     # 回测结束日期,date格式

        totalDays = len(df)                                        # 交易日数量
        profitDays = len(df[df['netPnl'] > 0])                     # 盈利交易日数量
        lossDays = len(df[df['netPnl'] < 0])                       # 亏损交易日数量

        endBalance = df['capital'].iloc[-1]                        # 最终总资产
        maxBalance = df['capital'].max()                           # 总资产最大值
        maxDf = df[df["capital"] == maxBalance]
        maxBalanceDate = maxDf.index.tolist()[0]                   # 总资产最大值的日期

        maxDrawdown = df['drawdown'].min()                         # 最大回撤金额
        maxDrawdownRate = df['drawdownR'].min()                    # 最大回撤比例

        totalNetPnl = df['netPnl'].sum()                           # 净利润总额
        dailyNetPnl = totalNetPnl / totalDays                      # 平均每个交易日净利润

        totalReturn = endBalance / self.initial_cash - 1           # 总利润率
        dailyReturn = df['capital_pct'].mean()                     # 日收益率均值
        returnStd = df['capital_pct'].std()                        # 日收益率标准差
        noRiskRate = 0.06                                          # 无风险利率, 这里设为6%
        noRiskRateDaily = pow((noRiskRate + 1), 1.0 / 240) - 1     # 无风险利率(日)

        # 计算夏普比例
        if returnStd:
            sharpeRatio = (dailyReturn - noRiskRateDaily) / returnStd * np.sqrt(240)
        else:
            sharpeRatio = 0

        years = totalDays / 250                                    # 回测的年份

        # 计算收益率,最大回撤比例,收益风险比
        yearReturn = pow((totalReturn + 1), 1.0 / years) - 1       # 年化收益率(复利)
        if abs(maxDrawdownRate) == 0:
            returnRisk = 0
        else:
            returnRisk = yearReturn / abs(maxDrawdownRate)         # 收益风险比(复利)

        d = dict()
        # 时间相关
        d['startDate'] = startDate                                 # 开始日期,date
        d['endDate'] = endDate                                     # 结束日期,date
        d['totalDays'] = totalDays                                 # 总交易日,int
        d['years'] = years                                         # 年份,float
        d['profitDays'] = profitDays                               # 盈利交易日数量
        d['lossDays'] = lossDays                                   # 亏损交易日数量
        # 利润成本相关
        d['totalNetPnl'] = totalNetPnl                             # 净利润总额
        d['dailyNetPnl'] = dailyNetPnl                             # 每日平均净利润
        # 收益风险相关
        d['endBalance'] = endBalance                               # 期末总资产
        d['maxBalance'] = maxBalance                               # 总资产最大值
        d['maxBalanceDate'] = maxBalanceDate                       # 总资产最大值的日期
        d['maxDrawdown'] = maxDrawdown                             # 最大回撤金额
        d['maxDrawdownRate'] = maxDrawdownRate                     # 最大回撤比例
        d['totalReturn'] = totalReturn                             # 总收益率
        d['yearReturn'] = yearReturn                               # 年化收益率(复利)
        d['returnRisk'] = returnRisk                               # 收益风险比(复利)
        d['sharpeRatio'] = sharpeRatio                             # 夏普比例

        self.dailyEvaluate = d                                     # 日线交易结果评价指标(收益风险比等)

    # ----------------------------------------------------------------
    def __print_save(self, content):
        """内置函数, 打印和保存"""
        dt = datetime.datetime.now()
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
        self.cal_t_result()
        self.cal_daily_result()
        # 时间相关
        self.__print_save(u"-----------时间相关-------------")
        self.__print_save(u"策略开始日期: {}".format(self.dailyEvaluate["startDate"]))
        self.__print_save(u"策略结束日期: {}".format(self.dailyEvaluate["endDate"]))
        totalDays = self.dailyEvaluate["totalDays"]
        self.__print_save(u"交易日总数量: {}".format(totalDays))
        self.__print_save(u"盈利交易日数量: {}, 占比: {:.1f}%".format(self.dailyEvaluate["profitDays"],
                                                             self.dailyEvaluate["profitDays"] / totalDays * 100))
        self.__print_save(u"亏损交易日数量: {}, 占比: {:.1f}%".format(self.dailyEvaluate["lossDays"],
                                                             self.dailyEvaluate["lossDays"] / totalDays * 100))
        self.__print_save(u"交易日总数量: {}".format(self.dailyEvaluate["totalDays"]))
        self.__print_save(u"年份: {:.1f}".format(self.dailyEvaluate["years"]))

        # 交易相关
        self.__print_save(u"")
        self.__print_save(u"-----------交易相关-------------")
        self.__print_save(u"T策略胜率: {:.2f}%".format(self.tradeEvaluate["winRate"] * 100))
        self.__print_save(u"T策略盈亏比: {:.2f}".format(self.tradeEvaluate["profitLossRatio"]))

        # 成本相关
        self.__print_save(u"")
        self.__print_save(u"-----------成本相关-------------")
        self.__print_save(u"净利润总额: {:.2f}".format(self.dailyEvaluate["totalNetPnl"]))

        # 收益相关
        self.__print_save(u"")
        self.__print_save(u"-----------收益相关-------------")
        self.__print_save(u"期初总资产: {:.2f}".format(self.initial_cash))
        self.__print_save(u"期末总资产: {:.2f}".format(self.dailyEvaluate["endBalance"]))
        self.__print_save(u"总收益率: {:.2f}%".format(self.dailyEvaluate["totalReturn"] * 100))
        self.__print_save(u"年化收益率: {:.2f}%".format(self.dailyEvaluate["yearReturn"] * 100))

        # 风险相关
        self.__print_save(u"")
        self.__print_save(u"-----------风险相关-------------")
        self.__print_save(u"总资产最大值: {:.2f}".format(self.dailyEvaluate["maxBalance"]))
        self.__print_save(u"总资产最大值的日期: {}".format(self.dailyEvaluate["maxBalanceDate"]))
        self.__print_save(u"最大回撤金额: {:.2f}".format(self.dailyEvaluate["maxDrawdown"]))
        self.__print_save(u"最大回撤比例: {:.2f}%".format(self.dailyEvaluate["maxDrawdownRate"] * 100))

        # 综合评价
        self.__print_save(u"")
        self.__print_save(u"-----------综合评价-------------")
        self.__print_save(u"收益风险比: {:.2f}".format(self.dailyEvaluate["returnRisk"]))
        self.__print_save(u"夏普比率: {:.2f}".format(self.dailyEvaluate["sharpeRatio"]))

    # ----------------------------------------------------------------
    def show(self, isSave=True):
        try:
            sns.set_style('whitegrid')
        except ImportError:
            pass
        # 画出资产曲线和最大回撤图
        fig = plt.figure(figsize=(10, 16))
        # 日线总资产
        df = self.show_df
        df.index = pd.to_datetime(df.index)
        ax1 = fig.add_subplot(1, 1, 1)
        x = list(df.index)
        y1 = df['available'].values
        y2 = df['capital'].values
        ax2 = ax1.twinx()
        ax1.plot(x, y1, 'r-', label="available", linewidth=2)
        ax2.plot(x, y2, 'b-', label="capital", linewidth=1)
        ax1.legend(loc='upper right')
        ax2.legend(loc='upper left')
        plt.show()

        fig2 = plt.figure(figsize=(10, 16))
        # 收益率曲线
        ax3 = fig2.add_subplot(1, 1, 1)
        x = list(df.index)
        y1 = df['cumprod_available_pct'].values
        y2 = df['cumprod_pct'].values
        ax4 = ax3.twinx()
        ax3.plot(x, y1, 'r-', label="available_rts", linewidth=2)
        ax4.plot(x, y2, 'b-', label="capital_rts", linewidth=1)
        ax3.legend(loc='upper right')
        ax4.legend(loc='upper left')
        plt.show()

        if isSave:
            Path = os.path.abspath(os.path.join(self.strategyPath, "figure_net_values.png"))
            Path2 = os.path.abspath(os.path.join(self.strategyPath, "figure_returns.png"))
            fig.savefig(Path)
            fig2.savefig(Path2)

