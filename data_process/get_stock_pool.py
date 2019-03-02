import json
from data_process.base_define import hs300_sh_address, trading_days_address


def get_stocks(address):
    with open(address, 'r') as f:
        stock_list = json.load(f)
    return stock_list


def get_trading_days(address):
    with open(address, 'r') as f:
        trading_days = json.load(f)
    return trading_days


if __name__ == '__main__':
    print(get_stocks(hs300_sh_address))
    print(get_trading_days(trading_days_address))
