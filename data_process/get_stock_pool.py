import json


def get_stocks(address):
    with open(address, 'r') as f:
        stock_list = json.load(f)
    return stock_list


if __name__ == '__main__':
    address = r'H:\vnpyStockEngine\data_process\sz_stock_list.json'
    print(get_stocks(address))

