import time
import datetime
import numpy as np
import pandas as pd
import warnings
from data_process.log_auth import client
from data_process.get_stock_tick import get_trans, get_wash_trans
from data_process.get_stock_params import get_stocks, get_trading_days
from data_process.base_define import hs300_sh_address, hs300_sz_address, trading_days_address, save_path
from data_process.plot_kline import ShowKline


def get_large_transaction(data, frequency, threshold):
    """
    :param df: 成交数据
    :param frequency:聚合的时间频率
    :param threshold:大单阈值
    :return:只含有大单的K线数据
    """
    df = data[['deal_time', 'deal_price', 'deal_volume']]
    df.set_index('deal_time', inplace=True)
    # 过滤撤单的情况，此时成交价为0
    df = df[df['deal_price'] != 0]
    # 只取出成交量大于阈值的量，同时此时的价格亦空值化，待根据留存的价格进行前向填充
    df['deal_volume'] = df['deal_volume'].apply(lambda x: x if x >= threshold else np.NaN)
    df.loc[np.isnan(df['deal_volume']), 'deal_price'] = np.NAN
    # 成交量0值填充
    df['deal_volume'].fillna(0, inplace=True)
    # 重塑K线
    df_new = df.resample(frequency)
    # 开盘价
    _open = df_new['deal_price'].first()
    # 最高价
    _high = df_new['deal_price'].max()
    # 最低价
    _low = df_new['deal_price'].min()
    # 收盘价
    _close = df_new['deal_price'].last()
    # 成交额
    _volume = df_new['deal_volume'].sum()
    df_last = pd.concat([_open, _high, _low, _close, _volume], axis=1)
    df_last.columns = ['open', 'high', 'low', 'close', 'volume']
    df_last.fillna(method='pad', inplace=True)
    return df_last


def get_data_filter_by_time(df):
    """过滤午盘的休息时间"""
    # 转换时间戳
    df['date'] = list(df.index)
    date = df['date'].iloc[-1].date()
    start_time1 = datetime.datetime(date.year, date.month, date.day, 9, 30, 0)
    end_time1 = datetime.datetime(date.year, date.month, date.day, 11, 30, 0)
    start_time2 = datetime.datetime(date.year, date.month, date.day, 13, 0, 0)
    end_time2 = datetime.datetime(date.year, date.month, date.day, 15, 0, 0)
    stk_data1 = df[(df['date'] >= start_time1) & (df['date'] <= end_time1)]
    stk_data2 = df[(df['date'] >= start_time2) & (df['date'] <= end_time2)]
    df = pd.concat([stk_data1, stk_data2], axis=0)
    df.drop(['date'], axis=1, inplace=True)
    return df


if __name__ == '__main__':
    trading_days = get_trading_days(trading_days_address)
    sh_stock_list = get_stocks(hs300_sh_address)
    sz_stock_list = get_stocks(hs300_sz_address)
    tradingHMS = '100000'

    t0 = time.time()
    for d in trading_days[:1]:
        startTime = datetime.datetime.strptime(d + tradingHMS, '%Y%m%d%H%M%S')
        endTime = datetime.datetime.strptime(d + tradingHMS, '%Y%m%d%H%M%S')
        for s in sz_stock_list[:1]:
            df = get_wash_trans(get_trans(client, startTime, endTime, s, 5), market='SZ', date=d)
            df_last = get_large_transaction(df, '300s', 10000)
            df_last = get_data_filter_by_time(df_last)
            # 展示重构后的K线
    chart = ShowKline(s)
    # 初始化数据
    chart.chart_init(df_last)
    # 显示图表
    chart.show_chart(height=600, width=1000)
    print(time.time() - t0)
