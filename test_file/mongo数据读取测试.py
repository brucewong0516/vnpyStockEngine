import pymongo
from datetime import datetime
from backtest.object import BarData
dataClass = BarData
client = pymongo.MongoClient(host='192.168.0.107')
db = client['crypto_1day']
collection = db['bitfinex_btc_usd']

flt = {'start_time': {'$gte': datetime.strptime('20151010', '%Y%m%d'),
                      '$lt': datetime.strptime('20151210', '%Y%m%d')}}

initCursor = collection.find(flt).sort('start_time', pymongo.ASCENDING)
# print(list(initCursor))
classList = []
for d in initCursor:
    bar = dataClass()
    bar.__dict__ = d
    classList.append(bar)
    print(d)
print(classList)
