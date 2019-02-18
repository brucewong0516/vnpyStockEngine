import pandas as pd
import datetime
from JZpyapi.JZpyapi.client import SyncSocketClient
from JZpyapi.JZpyapi.const import AUTH_TYPE_SERVER
from JZpyapi.JZpyapi.apis.tick_report import TickData

client = SyncSocketClient("192.168.0.240:9970",
                          auth_username="lqm", auth_password="lqm",  pid=82,
                          valid_date=2162706732, auth_type=AUTH_TYPE_SERVER)
# 获取L2数据
b = int(datetime.datetime.timestamp(datetime.datetime(2018, 12, 4, 10, 0, 0)))
e = int(datetime.datetime.timestamp(datetime.datetime(2018, 12, 4, 10, 0, 0)))
data = TickData.sync_request(client, b, e, "SZ300608", 2)
reports = pd.DataFrame(list(data.msg))

print(reports)


