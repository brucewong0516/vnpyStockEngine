import pandas as pd
import datetime
import time
import numpy as np
import os

from JZpyapi.JZpyapi.apis.tick_report import ReportData
from JZpyapi.JZpyapi.apis.tick_order import OrderData
from JZpyapi.JZpyapi.apis.tick_trans import TransData

from data_process.log_auth import client
from data_process.get_stock_params import get_stocks, get_trading_days
from data_process.base_define import hs300_sh_address, hs300_sz_address, trading_days_address, save_path


def get_tick(client, start_time, end_time, stock, market_type):
    # 获取L2数据
    """
    :param client: 服务器
    :param start_time: 起始时间，datetime格式
    :param end_time: 终止时间，datetime格式
    :param stock: 股票标的
    :param market_type: 上海的report数据是1，深市的report数据是2
    :return:
    """
    t0 = time.time()
    start_time_stamp = int(datetime.datetime.timestamp(start_time))
    end_time_stamp = int(datetime.datetime.timestamp(end_time))
    data = ReportData.sync_request(client, start_time_stamp, end_time_stamp, stock, market_type)
    print('读取{}report数据时间：{}'.format(stock, time.time() - t0))
    df = pd.DataFrame(list(data.msg))
    # print('数据长度{}'.format(len(df)))
    return df


def get_order(client, start_time, end_time, stock, market_type):
    # 获取L2数据
    """
    :param client: 服务器
    :param start_time: 起始时间，datetime格式
    :param end_time: 终止时间，datetime格式
    :param stock: 股票标的
    :param market_type: 上海的report数据是1，深市的report数据是2
    :return:
    """
    t0 = time.time()
    start_time_stamp = int(datetime.datetime.timestamp(start_time))
    end_time_stamp = int(datetime.datetime.timestamp(end_time))
    data = OrderData.sync_request(client, start_time_stamp, end_time_stamp, stock, market_type)
    print('读取{}order数据时间：{}'.format(stock, time.time() - t0))
    df = pd.DataFrame(list(data.msg))
    # print('数据长度{}'.format(len(df)))
    return df


def get_trans(client, start_time, end_time, stock, market_type):
    # 获取L2数据
    """
    :param client: 服务器
    :param start_time: 起始时间，datetime格式
    :param end_time: 终止时间，datetime格式
    :param stock: 股票标的
    :param market_type: 上海的report数据是4，深市的report数据是5
    :return:
    """
    t0 = time.time()
    start_time_stamp = int(datetime.datetime.timestamp(start_time))
    end_time_stamp = int(datetime.datetime.timestamp(end_time))
    data = TransData.sync_request(client, start_time_stamp, end_time_stamp, stock, market_type)
    print('读取{}trans数据时间：{}'.format(stock, time.time() - t0))
    df = pd.DataFrame(list(data.msg))
    return df


def get_wash_tick(df, market='SZ'):
    t0 = time.time()

    # 对L2数据进行清洗
    def get_msg_to_dict(msg):
        """
        :param msg: 对应的如report_data_rep_msg格式的msg内容，需要通过.key取出对应的value
        :return: 数据列表
        """
        # code // 代码
        # short_name // 简称
        # deal_status // 交易状态
        # deal_time // 交易时间
        # last_close // 昨收价
        # open // 开盘价
        # close // 收盘价
        # low // 最低价
        # new // 最新价
        # volumn // 成交量
        # over // 成交额
        # cjbs // 成交笔数
        # wp // 外盘
        # wmzl // 委买总量
        # wmjqpjj // 委买加权平均价
        # wszl // 委卖总量
        # wsjqpjj // 委卖加权平均价
        # sfxncj // 是否虚拟成交
        # wmds // 委卖档数
        # wmj // 档委卖价
        # wml // 档委卖量
        # wms // 档委卖档数
        # ztj // 涨停价
        # dtj // 跌停价
        # buy_yzwtbs // 买一总委托笔数
        # buy_yjssl // 买一揭示数量
        # sell_yzwtbs // 卖一总委托笔数
        # sell_yjssl // 卖一揭示数量
        # mbuyds // 委买档数
        # wbuyj // 当委买价
        # wbuyl // 档委买量
        # wbuys // 档委买档数

        return [msg.code, msg.deal_status, msg.deal_time, msg.last_close, msg.open, msg.close, msg.low, msg.new,
                msg.volumn, msg.over, msg.cjbs, msg.wmzl, msg.wmjqpjj, msg.wszl, msg.wsjqpjj,
                msg.wmj[0], msg.wmj[1], msg.wmj[2], msg.wmj[3], msg.wmj[4],
                msg.wmj[5], msg.wmj[6], msg.wmj[7], msg.wmj[8], msg.wmj[9],
                msg.wml[0], msg.wml[1], msg.wml[2], msg.wml[3], msg.wml[4],
                msg.wml[5], msg.wml[6], msg.wml[7], msg.wml[8], msg.wml[9],
                msg.wms[0], msg.wms[1], msg.wms[2], msg.wms[3], msg.wms[4],
                msg.wms[5], msg.wms[6], msg.wms[7], msg.wms[8], msg.wms[9],
                msg.wbuyj[0], msg.wbuyj[1], msg.wbuyj[2], msg.wbuyj[3], msg.wbuyj[4],
                msg.wbuyj[5], msg.wbuyj[6], msg.wbuyj[7], msg.wbuyj[8], msg.wbuyj[9],
                msg.wbuyl[0], msg.wbuyl[1], msg.wbuyl[2], msg.wbuyl[3], msg.wbuyl[4],
                msg.wbuyl[5], msg.wbuyl[6], msg.wbuyl[7], msg.wbuyl[8], msg.wbuyl[9],
                msg.wbuys[0], msg.wbuys[1], msg.wbuys[2], msg.wbuys[3], msg.wbuys[4],
                msg.wbuys[5], msg.wbuys[6], msg.wbuys[7], msg.wbuys[8], msg.wbuys[9],
                ]
    # 设定字段
    key_list = ['symbol', 'deal_status', 'date', 'last_close', 'open', 'high', 'low', 'close',
                'volume', 'money', 'tradedCount', 'totalDeputeBuy', 'averageBuy', 'totalDeputeSell', 'averageSell',
                'askP1', 'askP2', 'askP3', 'askP4', 'askP5',
                'askP6', 'askP7', 'askP8', 'askP9', 'askP10',
                'askV1', 'askV2', 'askV3', 'askV4', 'askV5',
                'askV6', 'askV7', 'askV8', 'askV9', 'askV10',
                'askD1', 'askD2', 'askD3', 'askD4', 'askD5',
                'askD6', 'askD7', 'askD8', 'askD9', 'askD10',
                'bidP1', 'bidP2', 'bidP3', 'bidP4', 'bidP5',
                'bidP6', 'bidP7', 'bidP8', 'bidP9', 'bidP10',
                'bidV1', 'bidV2', 'bidV3', 'bidV4', 'bidV5',
                'bidV6', 'bidV7', 'bidV8', 'bidV9', 'bidV10',
                'bidD1', 'bidD2', 'bidD3', 'bidD4', 'bidD5',
                'bidD6', 'bidD7', 'bidD8', 'bidD9', 'bidD10',
                ]
    df = pd.DataFrame([get_msg_to_dict(df.iloc[i, 0]) for i in range(len(df))], columns=key_list)
    # df.to_csv('./xxxx.csv')
    if market == 'SZ':
        df['date'] = df['date'].apply(lambda x: datetime.datetime.strptime((str(int(int(x)/1000))), '%Y%m%d%H%M%S'))
    elif market == 'SH':
        df['date'] = df['date'].apply(lambda x: datetime.datetime.fromtimestamp(int(x)))
    df.sort_values(by=['date'], ascending=True, inplace=True)
    # --------------------------------------------------------------------------------------------------------------
    # 委买委卖价调整
    df[['last_close', 'open', 'high', 'low', 'close',
        'averageBuy', 'averageSell',
        'askP1', 'askP2', 'askP3', 'askP4', 'askP5',
        'askP6', 'askP7', 'askP8', 'askP9', 'askP10',
        'bidP1', 'bidP2', 'bidP3', 'bidP4', 'bidP5',
        'bidP6', 'bidP7', 'bidP8', 'bidP9', 'bidP10']] = \
        df[['last_close', 'open', 'high', 'low', 'close',
            'averageBuy', 'averageSell',
            'askP1', 'askP2', 'askP3', 'askP4', 'askP5',
            'askP6', 'askP7', 'askP8', 'askP9', 'askP10',
            'bidP1', 'bidP2', 'bidP3', 'bidP4', 'bidP5',
            'bidP6', 'bidP7', 'bidP8', 'bidP9', 'bidP10']].apply(lambda x: x / 10000)
    # print('清洗数据时间：{}'.format(time.time() - t0))
    return df


def get_wash_order(df, market='SZ'):
    t0 = time.time()

    # 对L2数据进行清洗
    def get_msg_to_dict(msg):
        """
        :param msg: 对应的如report_data_rep_msg格式的msg内容，需要通过.key取出对应的value
        :return: 数据列表
        """
        # pdh // 频道号
        # xxjlh // 消息记录号
        # code // 证券代码
        # wtjg // 委托价格
        # wtsl // 委托数量
        # direction // 买卖方向
        # order_type // 订单类型
        # wtsj // 委托时间

        return [msg.pdh, msg.xxjlh, msg.code, msg.wtjg, msg.wtsl,
                msg.direction, msg.order_type, msg.wtsj
                ]
    # 设定字段
    key_list = ['channel', 'message_number',
                'code', 'commission_price',
                'commission_volume', 'direction',
                'order_type', 'commission_time'
                ]
    df = pd.DataFrame([get_msg_to_dict(df.iloc[i, 0]) for i in range(len(df))], columns=key_list)
    if market == 'SZ':
        df['commission_time'] = df['commission_time'].apply(lambda x: datetime.datetime.strptime((str(int(x))),
                                                                                                 '%Y%m%d%H%M%S%f'))
    df['message_number'] = df['message_number'].astype(int)
    df.sort_values(by=['message_number'], ascending=True, inplace=True)
    # print('清洗数据时间：{}'.format(time.time() - t0))
    return df


def get_wash_trans(df, market='SZ', date=''):
    t0 = time.time()

    # 对L2数据进行清洗
    def get_msg_to_dict(msg):
        """
        :param msg: 对应的如report_data_rep_msg格式的msg内容，需要通过.key取出对应的value
        :return: 数据列表
        """
        # cjxlh // 成交序列号
        # pdh // 频道号
        # code // 证券代码
        # deal_time // 成交时间
        # deal_price // 成交价格
        # deal_volumn // 成交数量
        # buy_syh // 买方挂单索引号
        # sell_syh // 卖方挂单索引号
        # deal_type // 成交类别

        return [msg.cjxlh, msg.pdh, msg.code, msg.deal_time, msg.deal_price,
                msg.deal_volumn, msg.buy_syh, msg.sell_syh, msg.deal_type
                ]
    # 设定字段
    key_list = ['transaction_number', 'channel',
                'code', 'deal_time',
                'deal_price', 'deal_volume',
                'buy_syh', 'sell_syh',
                'deal_type'
                ]
    df = pd.DataFrame([get_msg_to_dict(df.iloc[i, 0]) for i in range(len(df))], columns=key_list)

    if market == 'SZ':
        df['deal_time'] = df['deal_time'].apply(lambda x: datetime.datetime.strptime((str(int(x))), '%Y%m%d%H%M%S%f'))
    elif market == 'SH':
        df['deal_time'] = df['deal_time'].apply(lambda x: x if len(x) == 8 else '0'+x)
        df['deal_time'] = df['deal_time'].apply(lambda x: datetime.datetime.strptime(date + '' + x, '%Y%m%d%H%M%S%f'))
    df['transaction_number'] = df['transaction_number'].astype(int)
    df.sort_values(by=['transaction_number'], ascending=True, inplace=True)
    # print('清洗数据时间：{}'.format(time.time() - t0))
    return df


def get_data_filter_by_time(df):
    # 转换时间戳
    date = df['date'].iloc[-1].date()
    start_time1 = datetime.datetime(date.year, date.month, date.day, 9, 30, 0)
    end_time1 = datetime.datetime(date.year, date.month, date.day, 11, 30, 00)
    start_time2 = datetime.datetime(date.year, date.month, date.day, 13, 0, 0)
    end_time2 = datetime.datetime(date.year, date.month, date.day, 15, 0, 0)
    stk_data1 = df[(df['date'] >= start_time1) & (df['date'] <= end_time1)]
    stk_data2 = df[(df['date'] >= start_time2) & (df['date'] <= end_time2)]
    df = pd.concat([stk_data1, stk_data2], axis=0)
    df.index = df['date']
    # print('{} 起始第一个时间 {}'.format(df.iloc[0, 0], df.iloc[0, 1]))
    return df


def fill_data(df):
    t0 = time.time()
    date = datetime.datetime.utcfromtimestamp((df['date'].values[0]-np.datetime64('1970-01-01T00:00:00Z')) /\
                                              np.timedelta64(1, 's')).strftime('%Y-%m-%d %H:%M:%S')
    time_initial = date.split(' ')[1]
    date = date.split(' ')[0]
    # df.set_index('date', inplace=True)

    if time_initial == '09:30:00':
        hms_start1 = ' '.join([date, time_initial])
        hms_start2 = ' '.join([date, '13:00:00'])
    elif time_initial == '09:30:01':
        hms_start1 = ' '.join([date, time_initial])
        hms_start2 = ' '.join([date, '13:00:01'])
    elif time_initial == '09:30:02':
        hms_start1 = ' '.join([date, time_initial])
        hms_start2 = ' '.join([date, '13:00:02'])
    hms_end1 = ' '.join([date, '11:30:00'])
    hms_end2 = ' '.join([date, '15:00:00'])
    t1 = list(pd.date_range(start=hms_start1, end=hms_end1, freq='3s'))
    t2 = list(pd.date_range(start=hms_start2, end=hms_end2, freq='3s'))
    df_new = pd.DataFrame(index=t1 + t2)
    df_new = pd.concat([df, df_new], axis=1)
    df_new.fillna(method='ffill', inplace=True)
    df_new.drop([df_new.index.values[1]], inplace=True)
    # print('生成完整表花销时间 {}'.format(time.time()-t0))
    return df_new


def save_sh_data(stock_list, trading_days, save_path):
    tradingHMS = '100000'
    for d in trading_days:
        report_path = save_path.format('report', d)
        trans_path = save_path.format('trans', d)
        if not os.path.exists(report_path):
            os.makedirs(report_path)
        if not os.path.exists(trans_path):
            os.makedirs(trans_path)
        startTime = datetime.datetime.strptime(d + tradingHMS, '%Y%m%d%H%M%S')
        endTime = datetime.datetime.strptime(d + tradingHMS, '%Y%m%d%H%M%S')
        for s in stock_list:
            try:
                """获取report数据"""
                df = get_wash_tick(get_tick(client, startTime, endTime, s, 1), market='SH')
                # fq: data_process/report_data/date/stock.csv
                df.to_csv(report_path + os.sep + '{}.csv'.format(s))
                """获取trans数据"""
                df1 = get_wash_trans(get_trans(client, startTime, endTime, s, 4), market='SH', date=d)
                df1.to_csv(trans_path + os.sep + '{}.csv'.format(s))
            except Exception as e:
                print('{} is None'.format(s))
                continue


def save_sz_data(stock_list, trading_days, save_path):
    tradingHMS = '100000'
    for d in trading_days:
        report_path = save_path.format('report', d)
        trans_path = save_path.format('trans', d)
        order_path = save_path.format('order', d)
        if not os.path.exists(report_path):
            os.makedirs(report_path)
        if not os.path.exists(trans_path):
            os.makedirs(trans_path)
        if not os.path.exists(order_path):
            os.makedirs(order_path)
        startTime = datetime.datetime.strptime(d + tradingHMS, '%Y%m%d%H%M%S')
        endTime = datetime.datetime.strptime(d + tradingHMS, '%Y%m%d%H%M%S')
        for s in stock_list:
            try:
                """获取report数据"""
                df = get_wash_tick(get_tick(client, startTime, endTime, s, 2), market='SZ')
                # fq: data_process/report_data/date/stock.csv
                df.to_csv(report_path + os.sep + '{}.csv'.format(s))
                """获取order数据"""
                df = get_order(client, startTime, endTime, s, 3)
                df2 = get_wash_order(df, market='SZ')
                df2.to_csv(order_path + os.sep + '{}.csv'.format(s))
                """获取trans数据"""
                df1 = get_wash_trans(get_trans(client, startTime, endTime, s, 5), market='SZ', date=d)
                df1.to_csv(trans_path + os.sep + '{}.csv'.format(s))
            except Exception as e:
                print('{} is None'.format(s))
                continue


def get_last_tick_data(stock, trading_days):
    tradingHMS = '100000'
    for d in trading_days:
        startTime = datetime.datetime.strptime(d + tradingHMS, '%Y%m%d%H%M%S')
        endTime = datetime.datetime.strptime(d + tradingHMS, '%Y%m%d%H%M%S')
        try:
            """获取report数据"""
            df = get_wash_tick(get_tick(client, startTime, endTime, stock, 1), market='SH')
            print(df.iloc[-1, :])
        except Exception as e:
            print('{} is None'.format(stock))


if __name__ == '__main__':
    trading_days = get_trading_days(trading_days_address)  # [-5:]
    trading_days = ['20181204', '20181205', '20181206', '20181207', '20181210', '20181211', '20181212',
                    '20181213']
    sh_stock_list = get_stocks(hs300_sh_address)
    sz_stock_list = get_stocks(hs300_sz_address)

    t0 = time.time()
    get_last_tick_data(sh_stock_list[0], trading_days)
    # save_sh_data(sh_stock_list, trading_days, save_path)
    # save_sz_data(sz_stock_list, trading_days, save_path)
    print(time.time()-t0)

