import pandas as pd
import pickle
import os
from data_process.base_define import szfile_address_base


if __name__ == '__main__':
    count = 0
    szfile_list = os.listdir(szfile_address_base)
    file_list = szfile_list
    for file_name in file_list:
        count += 1
        file_address = szfile_address_base + os.sep + file_name
        df = pd.read_csv(file_address)
        # 获取可交易的上证指数成分股
        df_copy = df[(df['交易状态'] == 'T111') & (df['证券代码'] > 'SH600000 ') & (df['证券代码'] <= 'SH700000 ')]
        stock_list = list(set(df_copy['证券代码']))
        df_copy.to_csv('./sh_temp/trade_data{}.csv'.format(count))
    with open('./stock_list.pkl', 'wb') as f:
        pickle.dump(stock_list, f)
