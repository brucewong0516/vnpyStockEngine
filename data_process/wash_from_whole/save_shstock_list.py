import pandas as pd
import pickle
import os
from data_process.base_define import shfile_address_base


def save_data(date):
    count = 0
    shfile_list = os.listdir(shfile_address_base.format(date))
    for file_name in shfile_list:
        count += 1
        file_address = shfile_address_base.format(date) + os.sep + file_name
        df = pd.read_csv(file_address)
        # 获取可交易的上证指数成分股
        df_copy = df[(df['交易状态'] == 'T111') & (df['证券代码'] > 'SH600000 ') & (df['证券代码'] <= 'SH700000 ')]
        stock_list = list(set(df_copy['证券代码']))
        df_copy.to_csv('./sh_temp/trade_data{}.csv'.format(count))
    with open('./stock_list.pkl', 'wb') as f:
        pickle.dump(stock_list, f)
    return stock_list
#
# if __name__ == '__main__':
#     from data_process.base_define import date_list
#     save_data(date_list[0])


