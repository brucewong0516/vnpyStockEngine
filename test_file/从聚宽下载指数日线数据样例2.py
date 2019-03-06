import jqdatasdk
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
    df['frequency'] = '1day'
    df['start_time'] = list(df.index)
    df['start_time'] = df['start_time'].apply(lambda x: datetime.datetime.strftime(x + datetime.timedelta(hours=9.5),
                                                                                   '%Y-%m-%d %H:%M:%S'))
    df['start_time'] = df['start_time'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df['end_time'] = df['start_time'] + datetime.timedelta(hours=5.5)
    df['amount'] = df['money']
    df['openInterest'] = 0
    df = df[['symbol', 'frequency',
             'start_time', 'end_time',
             'open', 'high',
             'low', 'close',
             'volume', 'amount',
             'openInterest']]
    df.dropna(axis=0, inplace=True)
    return df


if __name__ == '__main__':
    data = get_price('000001.XSHG', start_date='2010-01-01', end_date='2019-01-01')
    data = wash_data(data)
    client = pymongo.MongoClient("192.168.0.107", port=27017)
    db = client['stock_1day']
    collection = db['000002.XSHG']
    data_list = list(data.T.to_dict().values())
    collection.insert_many(data_list)

