import os
import time
import numpy as np
import pandas as pd
import pickle
import pymongo
import datetime
import warnings
from multiprocessing.dummy import Pool
from data_process.base_define import trade_data_base, stock_list_address
warnings.filterwarnings('ignore')


def set_time_df(hour, count):
    """根据小时和分钟设定一个df"""
    if hour == 9:
        df = pd.DataFrame(np.arange(count,60), index=count * [hour], columns=['minute'])
    else:
        df = pd.DataFrame(np.arange(count), index=count*[hour], columns=['minute'])
    df['hour'] = df.index.values
    return df[['hour', 'minute']]


def time_to_rank(hour, minute):
    """根据小时和分钟，返回对应的序列位置"""
    df1 = set_time_df(9, 30)
    df2 = set_time_df(10, 60)
    df3 = set_time_df(11, 30)
    df4 = set_time_df(13, 60)
    df5 = set_time_df(14, 60)
    df = pd.concat([df1, df2, df3, df4, df5], axis=0)
    df['rank'] = range(1, len(df)+1)
    df.reset_index(drop=True, inplace=True)
    return df.loc[(df['hour'] == hour) & (df['minute'] == minute), 'rank'].values[0]


def wash_data(stk_data):
    # --------------------------------------------------------------------------------------------------------------
    # 转换时间戳
    stk_data['时间'] = stk_data['时间'].apply(lambda x: datetime.datetime.fromtimestamp(x))
    date = stk_data['时间'].iloc[0].date()
    start_time1 = datetime.datetime(date.year, date.month, date.day, 9, 30, 0)
    end_time1 = datetime.datetime(date.year, date.month, date.day, 11, 30, 0)
    start_time2 = datetime.datetime(date.year, date.month, date.day, 13, 0, 0)
    end_time2 = datetime.datetime(date.year, date.month, date.day, 15, 0, 0)
    stk_data1 = stk_data[(stk_data['时间'] >= start_time1) & (stk_data['时间'] < end_time1)]
    stk_data2 = stk_data[(stk_data['时间'] >= start_time2) & (stk_data['时间'] < end_time2)]
    stk_data = pd.concat([stk_data1, stk_data2], axis=0)

    # --------------------------------------------------------------------------------------------------------------
    # 添加日期和分钟
    stk_data['date'] = stk_data['时间'].apply(lambda x: datetime.datetime.strftime(x.date(), '%Y-%m-%d'))
    stk_data['hour'] = stk_data['时间'].apply(lambda x: x.hour)
    stk_data['minute'] = stk_data['时间'].apply(lambda x: x.minute)
    stk_data['count'] = list(map(lambda x, y: time_to_rank(x, y), stk_data['hour'], stk_data['minute']))

    # --------------------------------------------------------------------------------------------------------------
    # 价格调整
    stk_data[['昨收价', '开盘价', '收盘价', '最低价', '最新价']] = \
        stk_data[['昨收价', '开盘价', '收盘价', '最低价', '最新价']].apply(lambda x: x / 10000)

    # --------------------------------------------------------------------------------------------------------------
    # 成交量（累计增量）==》 即时成交量
    vol_diff = stk_data['成交量'].diff()
    vol_diff = vol_diff.fillna(stk_data['成交量'].iloc[0])
    stk_data['成交量'] = vol_diff

    # --------------------------------------------------------------------------------------------------------------
    # 成交额（累计增量）==》 即时成交额
    amount_diff = stk_data['成交额'].diff()
    amount_diff = amount_diff.fillna(stk_data['成交额'].iloc[0])
    stk_data['成交额'] = amount_diff

    # --------------------------------------------------------------------------------------------------------------
    # 成交笔数（累计增量）==》 即时成交笔数
    amount_diff = stk_data['成交笔数'].diff()
    amount_diff = amount_diff.fillna(stk_data['成交笔数'].iloc[0])
    stk_data['成交笔数'] = amount_diff

    # --------------------------------------------------------------------------------------------------------------
    # 委买委卖价调整
    stk_data[['委买加权平均价', '委卖加权平均价',
              '1档委买价', '2档委买价', '3档委买价', '4档委买价', '5档委买价',
              '6档委买价', '7档委买价', '8档委买价', '9档委买价', '10档委买价',
              '1档委卖价', '2档委卖价', '3档委卖价', '4档委卖价', '5档委卖价',
              '6档委卖价', '7档委卖价', '8档委卖价', '9档委卖价', '10档委卖价']] = \
        stk_data[['委买加权平均价', '委卖加权平均价',
                  '1档委买价', '2档委买价', '3档委买价', '4档委买价', '5档委买价',
                  '6档委买价', '7档委买价', '8档委买价', '9档委买价', '10档委买价',
                  '1档委卖价', '2档委卖价', '3档委卖价', '4档委卖价', '5档委卖价',
                  '6档委卖价', '7档委卖价', '8档委卖价', '9档委卖价', '10档委卖价']].apply(lambda x: x / 10000)

    # --------------------------------------------------------------------------------------------------------------
    # 丢弃部分无效列
    stk_data.drop(['Unnamed: 0', '证券简称', '交易状态', '外盘', '是否虚拟成交', '委买档数', '委卖档数'], axis=1, inplace=True)

    # --------------------------------------------------------------------------------------------------------------
    # 重新定义索引
    stk_data.reset_index(inplace=True)

    # --------------------------------------------------------------------------------------------------------------
    # 重命名列
    stk_data.columns = ['symbol', 'time', 'pre_close', 'open', 'high', 'low', 'close', 'volume', 'amount',
                        'count', 'total_bid_volume', 'bid_ema_price', 'total_ask_volume', 'ask_ema_price',
                        'bid_price1', 'bid_volume1', 'bid_count1',
                        'bid_price2', 'bid_volume2', 'bid_count2',
                        'bid_price3', 'bid_volume3', 'bid_count3',
                        'bid_price4', 'bid_volume4', 'bid_count4',
                        'bid_price5', 'bid_volume5', 'bid_count5',
                        'bid_price6', 'bid_volume6', 'bid_count6',
                        'bid_price7', 'bid_volume7', 'bid_count7',
                        'bid_price8', 'bid_volume8', 'bid_count8',
                        'bid_price9', 'bid_volume9', 'bid_count9',
                        'bid_price10', 'bid_volume10', 'bid_count10',
                        'ask_price1', 'ask_volume1', 'ask_count1',
                        'ask_price2', 'ask_volume2', 'ask_count2',
                        'ask_price3', 'ask_volume3', 'ask_count3',
                        'ask_price4', 'ask_volume4', 'ask_count4',
                        'ask_price5', 'ask_volume5', 'ask_count5',
                        'ask_price6', 'ask_volume6', 'ask_count6',
                        'ask_price7', 'ask_volume7', 'ask_count7',
                        'ask_price8', 'ask_volume8', 'ask_count8',
                        'ask_price9', 'ask_volume9', 'ask_count9',
                        'ask_price10', 'ask_volume10', 'ask_count10',
                        'date', 'hour', 'minute', 'count'
                        ]
    # 过滤负值等无效数据
    stk_data = stk_data[stk_data['ask_volume1'] > 0]
    stk_data = stk_data[stk_data['ask_volume2'] > 0]
    stk_data = stk_data[stk_data['bid_volume1'] > 0]
    stk_data = stk_data[stk_data['bid_volume2'] > 0]

    return stk_data


def multi_wash(stock_list, data_sets):
    pool = Pool(processes=4)
    res_list = []
    for i in range(len(stock_list)):
        stk = stock_list[i]
        data = data_sets.loc[stk]
        res = pool.apply_async(wash_data, args=(data, ))
        res_list.append(res)
    for j in range(len(res_list)):
        print(stock_list[j])
        stk_data = res_list[j].get()
        collection = db[str(stock_list[j][2:]) + '.XSHG']
        data_list = list(stk_data.T.to_dict().values())
        if data_list:
            collection.insert_many(data_list)
            print('saved {} data done'.format(stock_list[j]))


if __name__ == '__main__':
    # --------------------------------------------------------------------------------------------------------------
    # 定义数据库
    client = pymongo.MongoClient(port=27017)
    db = client['stock_1tick']
    # 遍历文件
    file_list = os.listdir(trade_data_base)
    dfs = []
    for file_name in file_list:
        trade_data_address = trade_data_base + os.sep + file_name
        df = pd.read_csv(trade_data_address, index_col=1)
        dfs.append(df)
    df_last = pd.concat(dfs, axis=0)
    df_last.sort_values(by=['时间'], ascending=True)
    with open(stock_list_address, 'rb') as f:
        stock_list = pickle.load(f)
    # 过滤已经完成数据存储的股票
    stock_list = [x for x in stock_list if str(x[2:]) + '.XSHG' not in db.collection_names()]
    # 统计花销时间
    t0 = time.time()
    multi_wash(stock_list, df_last)
    print('解析文件{}花费时间{}'.format(trade_data_address, time.time()-t0))  # 总计26096s
