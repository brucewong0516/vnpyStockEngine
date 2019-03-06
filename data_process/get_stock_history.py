import pandas as pd

from data_process.log_auth import client_mysql_history


def get_min_data(stock, startTime=None, endTime=None, count=None):
    """
    :param count: 数据个数
    :param stock: 所需要查询的股票标的，'SZ300189'
    :param startTime: 所需要查询的时间起点，'2018-02-06 08:00:00'
    :param endTime: 所需要查询的时间终点，'2018-02-06 16:00:00'
    :return: 返回数据
    """

    cur = client_mysql_history.cursor()
    try:
        if count:
            # 如果传入了count这个参数，传入的起始时间决定不了数据的长度
            sql = "select * from mins where `code`='{}' and\n" \
                  " `time` BETWEEN '{}' and '{}' order by `time` desc limit {} ".format(stock,
                                                                                        startTime,
                                                                                        endTime,
                                                                                        count)
            cur.execute(sql)                          # 执行sql语句
            results = pd.DataFrame(cur.fetchall())    # 获取查询的所有记录
            results.set_index('time', inplace=True)
            results.sort_index(ascending=True, inplace=True)
        else:
            sql = "select * from mins where `code`='{}' and `time` BETWEEN '{}' and '{}'".format(stock,
                                                                                                 startTime,
                                                                                                 endTime)
            cur.execute(sql)  # 执行sql语句
            results = pd.DataFrame(cur.fetchall())  # 获取查询的所有记录
            results.set_index('time', inplace=True)
    except Exception as e:
        raise e
    finally:
        client_mysql_history.close()              # 关闭连接
    return results


if __name__ == '__main__':

    df = get_min_data('SZ300189', '2018-02-05 09:36:00', '2018-02-06 09:36:10', count=3)
    print(df)
