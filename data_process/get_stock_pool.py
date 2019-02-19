import json
from data_process.base_define import sh_address


def get_stocks(address):
    with open(address, 'r') as f:
        stock_list = json.load(f)
    return stock_list


if __name__ == '__main__':
    print(get_stocks(sh_address))

