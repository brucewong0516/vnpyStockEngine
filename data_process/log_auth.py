from JZpyapi.JZpyapi.client import SyncSocketClient
from JZpyapi.JZpyapi.const import AUTH_TYPE_SERVER

client = SyncSocketClient("192.168.0.240:9970",
                          auth_username="lqm", auth_password="lqm",  pid=82,
                          valid_date=2162706732, auth_type=AUTH_TYPE_SERVER)
