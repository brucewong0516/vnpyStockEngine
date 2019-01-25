# -*- coding: utf-8 -*-
"""
案例策略的参数优化
"""

from backtest.backtestEngine import BacktestingEngine, OptimizationSetting
from backtest.template import CtaTemplate
if __name__ == '__main__':
    # -------------------------------------------------------------------
    # 1、创建回测引擎
    engine = BacktestingEngine()

    # -------------------------------------------------------------------
    # 2、设置基本的变量
    # 设置数据库
    engine.setDatabase(address="192.168.0.107", port=27017)  # 本地的数据库
    # 设置初始资金
    engine.setInitCapital(1000000)  # 初始资金为100w元
    # 设置策略开始和结束时间
    engine.setStartDate('20140101', initDays=10)  # 策略开始日期, 加载多少天的数据
    engine.setEndDate('20181231')                # 策略结束日期

    # -------------------------------------------------------------------
    # 3、设置策略信息和参数
    # 设置策略基本信息
    setting = {"ClassName": "test",
               "market": "stock",
               "symbol": "000001.XSHG",
               "frequency": "1day"}
    # 传入引擎中
    engine.setStrategySetting(setting)
    # -------------------------------------------------------------------
    # 5、设置优化参数
    # 新建一个优化任务设置对象
    setting = OptimizationSetting()
    # 设置优化排序目标, 支持: 收益风险比:"returnRisk", 夏普比率:"sharpeRatio", 年化收益率:"yearReturn"
    setting.setOptimizeTarget("sharpeRatio")
    # 设置变动参数
    setting.addParameter('n1', 5, 10, 1)  # 增加第一个优化参数n1
    setting.addParameter('n2', 20, 30, 1)  # 增加第二个优化参数n2

    # -------------------------------------------------------------------
    # 6、多进程优化
    engine.runParallelOptimization(CtaTemplate, setting)



