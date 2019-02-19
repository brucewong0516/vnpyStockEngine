import pandas as pd
import datetime
import time
import numpy as np

from JZpyapi.JZpyapi.apis.tick_report import TickData
from data_process.log_auth import client
from data_process.get_stock_pool import get_stocks


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
    data = TickData.sync_request(client, start_time_stamp, end_time_stamp, stock, market_type)
    df = pd.DataFrame(list(data.msg))
    print('读取{}数据时间：{}'.format(stock, time.time()-t0))
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

        return [msg.code, msg.deal_time, msg.last_close, msg.open, msg.close, msg.low, msg.new,
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
    key_list = ['symbol', 'date', 'last_close', 'open', 'high', 'low', 'close',
                'volume', 'money', 'tradedCount', 'totalDeputeBuy', 'averageBuy', 'totalDeputeSell', 'averageSell',
                'sellP1', 'sellP2', 'sellP3', 'sellP4', 'sellP5',
                'sellP6', 'sellP7', 'sellP8', 'sellP9', 'sellP10',
                'sellV1', 'sellV2', 'sellV3', 'sellV4', 'sellV5',
                'sellV6', 'sellV7', 'sellV8', 'sellV9', 'sellV10',
                'sellD1', 'sellD2', 'sellD3', 'sellD4', 'sellD5',
                'sellD6', 'sellD7', 'sellD8', 'sellD9', 'sellD10',
                'askP1', 'askP2', 'askP3', 'askP4', 'askP5',
                'askP6', 'askP7', 'askP8', 'askP9', 'askP10',
                'askV1', 'askV2', 'askV3', 'askV4', 'askV5',
                'askV6', 'askV7', 'askV8', 'askV9', 'askV10',
                'askD1', 'askD2', 'askD3', 'askD4', 'askD5',
                'askD6', 'askD7', 'askD8', 'askD9', 'askD10',
                ]
    df = pd.DataFrame([get_msg_to_dict(df.iloc[i, 0]) for i in range(len(df))], columns=key_list)
    if market == 'SZ':
        df['date'] = df['date'].apply(lambda x: datetime.datetime.strptime((str(int(int(x)/1000))), '%Y%m%d%H%M%S'))
    elif market == 'SH':
        df['date'] = df['date'].apply(lambda x: datetime.datetime.fromtimestamp(int(x)))
    df.sort_values(by=['date'], ascending=True, inplace=True)
    print('清洗数据时间：{}'.format(time.time() - t0))
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
    print('{} 起始第一个时间 {}'.format(df.iloc[0, 0], df.iloc[0, 1]))
    return df


def fill_data(df):
    t0 = time.time()
    date = datetime.datetime.utcfromtimestamp((df['date'].values[0]-np.datetime64('1970-01-01T00:00:00Z')) /\
                                              np.timedelta64(1, 's')).strftime('%Y-%m-%d %H:%M:%S')
    time_initial = date.split(' ')[1]
    date = date.split(' ')[0]
    df.set_index('date', inplace=True)
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
    print('生成完整表花销时间 {}'.format(time.time()-t0))
    return df_new


if __name__ == '__main__':
    start_time = datetime.datetime(2018, 12, 4, 10, 0, 0)
    end_time = datetime.datetime(2018, 12, 4, 10, 0, 0)

    stock_list = get_stocks(r'H:\vnpyStockEngine\data_process\sz_stock_list.json')
    t0 = time.time()
    for s in stock_list[1:10]:
        try:
            df = get_tick(client, start_time, end_time, s, 2)
            df2 = get_wash_tick(df, market='SZ')
            df3 = get_data_filter_by_time(df2)
            df4 = fill_data(df3)
        except Exception as e:
            print('{} is None'.format(s))
            continue
    print(time.time()-t0)
