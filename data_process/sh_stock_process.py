import os
import pandas as pd
from data_process.sh_base_define import trade_data_base, stock_list_address
import pickle
import pymongo
import datetime
import warnings
warnings.filterwarnings('ignore')


def wash_data(stk_data):
    # --------------------------------------------------------------------------------------------------------------
    # 转换时间戳
    stk_data['时间'] = stk_data['时间'].apply(lambda x: datetime.datetime.fromtimestamp(x))

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
                        'ask_price10', 'ask_volume10', 'ask_count10'
                        ]

    # path = stk_data['symbol'][0]
    # stk_data.to_csv('./{}.csv'.format(path))
    return stk_data


if __name__ == '__main__':
    # --------------------------------------------------------------------------------------------------------------
    # 定义数据库
    client = pymongo.MongoClient('192.168.0.107', port=27017)
    db = client['stock_1tick']

    file_list = os.listdir(trade_data_base)
    for file_name in file_list:
        trade_data_address = trade_data_base + os.sep + file_name
        df = pd.read_csv(trade_data_address, index_col=1)
        with open(stock_list_address, 'rb') as f:
            stock_list = pickle.load(f)
        for stk in stock_list[:1]:
            stk_data = wash_data(df.loc[stk])
            stk_data = stk_data[stk_data['ask_volume1'] > 0]
            stk_data = stk_data[stk_data['ask_volume2'] > 0]
            stk_data = stk_data[stk_data['bid_volume1'] > 0]
            stk_data = stk_data[stk_data['bid_volume2'] > 0]
            collection = db[str(stk[2:]) + '.XSHG']
            data_list = list(stk_data.T.to_dict().values())
            collection.insert_many(data_list)
            print('OK')


