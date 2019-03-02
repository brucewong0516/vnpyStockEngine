import time
import datetime
from data_process.log_auth import client
from data_process.get_stock_tick import get_trans, get_wash_trans
from data_process.get_stock_pool import get_stocks, get_trading_days
from data_process.base_define import hs300_sh_address, hs300_sz_address, trading_days_address, save_path


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
    # df.to_csv('xxx.csv')
    print(time.time() - t0)
