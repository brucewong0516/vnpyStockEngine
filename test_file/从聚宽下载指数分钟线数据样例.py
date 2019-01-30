import copy
import datetime
from jqdatasdk import *
import pymongo
auth('18665304480', '911002yangmi')


class DataObject(object):
    def __init__(self):
        self.symbol = ''
        self.frequency = ''
        self.start_time = None
        self.end_time = None
        self.open = 0.0
        self.high = 0.0
        self.low = 0.0
        self.close = 0.0
        self.volume = 0.0
        self.amount = 0.0


def wash_data(df):
    df['symbol'] = 'bitfinex_btc_usd'
    df['frequency'] = '1min'
    df['end_time'] = list(df.index)
    df['end_time'] = df['end_time'].apply(lambda x: datetime.datetime.strftime(x, '%Y-%m-%d %H:%M:%S'))
    df['end_time'] = df['end_time'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df['start_time'] = df['end_time'] - datetime.timedelta(seconds=60)
    df['amount'] = df['money']
    df['openInterest'] = 0
    df = df[['symbol', 'frequency',
             'start_time', 'end_time',
             'open', 'high',
             'low', 'close',
             'volume', 'amount',
             'openInterest']]
    df_copy = copy.deepcopy(df)
    df_copy.dropna(axis=0, inplace=True)
    return df


if __name__ == '__main__':
    # 定义数据库
    # client = pymongo.MongoClient("192.168.0.107", port=27017)
    client = pymongo.MongoClient(port=27017)
    db = client['stock_1min']
    collection = db['000001.XSHG']
    # collection.create_index([("start_time", pymongo.ASCENDING)])
    # 设定存入的时间点
    start_date = datetime.date(2010, 1, 4)
    end_date = datetime.date(2019, 1, 3)
    trading_days = list(get_trade_days(start_date, end_date))
    for i in range(len(trading_days)-1):
        t0 = trading_days[i]
        t1 = trading_days[i+1]
        data = get_price('000001.XSHG', frequency='1m', start_date=t0, end_date=t1)
        data = wash_data(data)
        data_list = list(data.T.to_dict().values())
        collection.insert_many(data_list)

