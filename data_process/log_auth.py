from JZpyapi.JZpyapi.client import SyncSocketClient
from JZpyapi.JZpyapi.const import AUTH_TYPE_SERVER
import pymysql.cursors


def connection(database_key):
    mysql = {'host': '',
             'port': None,
             'user': '',
             'password': '',
             'db': '',
             'charset': '',
             'cursorclass': ''}
    if database_key == 'company':
        mysql['host'] = '120.77.75.168'
        mysql['user'] = 'fengpan'
        mysql['password'] = 'b46Zi5OeY5Qj1N8c4Kw0Ovn0jsnL7w'
        mysql['port'] = 4001
        mysql['charset'] = 'utf8mb4'
        mysql['db'] = 'history'
        mysql['cursorclass'] = pymysql.cursors.DictCursor
    return mysql


client = SyncSocketClient("192.168.0.240:9970",
                          auth_username="lqm", auth_password="lqm",  pid=82,
                          valid_date=2162706732, auth_type=AUTH_TYPE_SERVER)

client2 = SyncSocketClient("192.168.0.200:9970",
                           auth_username="lqm", auth_password="lqm",  pid=82,
                           valid_date=2162706732, auth_type=AUTH_TYPE_SERVER)

client_mysql_history = pymysql.connect(**connection('company'))

