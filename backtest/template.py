from backtest import constant as vc  # 常量模块
from backtest import object as vo    # 对象模块


class CtaTemplate(object):
    
    # ----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        # 设置回测或交易引擎
        self.ctaEngine = ctaEngine
        self.pos = 0               # 持仓情况
        # 参数列表，保存了参数的名称(策略被实例化的时候会从setting中读取)
        self.paramList = ['className', 'market',
                          'symbol', 'frequency']
        # 设置策略的参数
        if setting:
            d = self.__dict__
            for key in self.paramList:
                if key in setting:
                    d[key] = setting[key]
        # 策略参数
        self.n1 = setting["n1"]  # 短期均线参数
        self.n2 = setting["n2"]  # 长期均线参数
        # 其他变量
        self.orderList = []  # 所有委托代码列表
        self.maxDepositRate = 0  # 最高保证金仓位比例
        # 创建K线合成器对象,第一个参数"1min"就是合成目标bar的频率，self.onLargerBar就是更新合成目标bar的存储器
        self.barManagerLarge = vo.BarManager("1day", self.onLargerBar)
        # 创建K线序列管理对象
        # K线管理对象(小周期)
        # self.arrayManager = vo.ArrayManager(self.n2 + 1)
        # K线合成管理对象(大周期)
        self.arrayManagerLarge = vo.ArrayManager(self.n2 + 1)

    # ----------------------------------------------------------------------
    def onInit(self):
        initData = self.ctaEngine.loadBar()
        for bar in initData:
            # 通过initData里面的bar将初始的ArrayManager的0值k线数据化
            # 此时的self.inited = True完成初始化
            # self.onBar(bar)
            self.barManagerLarge.updateBar(bar)

    # ----------------------------------------------------------------------
    def onLargerBar(self, bar):
        """收到大级别K线数据"""
        self.arrayManagerLarge.updateBar(bar)
        # 排除日线数据不足的情况
        if not self.arrayManagerLarge.inited:
            return

    # ----------------------------------------------------------------------
    def onBar(self, bar):
        # 更新大周期K线
        self.barManagerLarge.updateBar(bar)
        # # 添加K线序列
        # self.arrayManager.updateBar(bar)
        # # 排除小周期K线序列不足的情况
        # if not self.arrayManager.inited:
        #     return
        # 收到新的K线后，撤销之前所有的限价单
        self.cancelAll()
        # -------------------------------------------------------------
        # 指标信号计算部分
        # 计算指标
        # 直接拿数据库能拿到的bar级别进行指标计算和回测,比如小周期就是5min不要合成
        # ma1 = self.arrayManager.sma(self.n1, array=False)
        # ma2 = self.arrayManager.sma(self.n2, array=False)
        # """合成的大级别回测"""
        ma1 = self.arrayManagerLarge.sma(self.n1, array=False)
        ma2 = self.arrayManagerLarge.sma(self.n2, array=False)
        # 计算信号
        trend_up = ma1 > ma2
        trend_dn = ma1 < ma2
        # -------------------------------------------------------------
        # 交易判断部分
        # 当前无仓位
        if self.pos == 0:
            account = self.ctaEngine.account
            # 趋势向上
            if trend_up:
                self.buy(bar.close, int(account.cash/bar.close))  # 开多
                print('{} buyPrice is {} '.format(bar.start_time, bar.close))
        # 当前持有多头
        elif self.pos > 0:
            # 趋势转下
            if trend_dn:
                # 平多
                self.sell(bar.close, abs(self.pos))
                print('{} sellPrice is {} '.format(bar.start_time, bar.close))

    # ----------------------------------------------------------------------
    def buy(self, price, volume):
        """买开"""
        return self.ctaEngine.sendOrder(self.symbol, vc.ORDER_BUY, price, volume)

    # ----------------------------------------------------------------------
    def sell(self, price, volume):
        """卖平"""
        return self.ctaEngine.sendOrder(self.symbol, vc.ORDER_SELL, price, volume)

    # ----------------------------------------------------------------------
    def cancelAll(self):
        """全部撤单"""
        self.ctaEngine.cancelAll()


