# -*- coding: utf-8 -*-
"""
案例策略运行
"""

import time
from backtest.backtestEngine import BacktestingEngine
from backtest.template import CtaTemplate

if __name__ == '__main__':
    t1 = time.time()
    # -------------------------------------------------------------------
    # 1、创建回测引擎
    engine = BacktestingEngine()
    # -------------------------------------------------------------------
    # 2、设置基本的变量
    # 设置数据库
    engine.setDatabase(address="localhost", port=27017)   # 远程数据库
    # 设置初始资金
    engine.setInitCapital(1000000)                # 初始资金为100w元
    # 设置策略开始和结束时间
    engine.setStartDate('20140101', initDays=30)  # 策略开始日期, 加载多少天的数据
    engine.setEndDate('20181231')                 # 策略结束日期
    # -------------------------------------------------------------------
    # 3、设置策略信息和参数
    # 设置策略基本信息
    """策略名称、市场、标的、频率"""
    setting = {"ClassName": "test",
               "market": "stock",
               "symbol": "000001.XSHG",
               "frequency": "1min"}
    # 设置交易参数
    d = {"n1": 6, "n2": 30}
    setting.update({"d": d})
    # 传入引擎中
    engine.setStrategySetting(setting)
    # -------------------------------------------------------------------
    # 5、执行回测，传入策略的类
    engine.runBacktesting(CtaTemplate)
    # -------------------------------------------------------------------
    # 6、策略评价
    engine.runEvaluating(saving=True, plotting=True)
    print(u"回测部分耗时:{:.2f}s".format(time.time()-t1))
