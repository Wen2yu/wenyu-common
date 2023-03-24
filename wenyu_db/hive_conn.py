# -*- coding: utf-8 -*-

# Name: hive_conn
# Product_name: PyCharm
# Description:
# Author: 'caoyitao'
# Date: 2021-06-11 17:41:03

# 导入模块
from pyhive import hive


# 定义函数
def __hive_conn__(sid):
    """
    //获取hive连接；
    """
    if sid == "dc":
        host = "10.10.11.228"
        port = 10000
        username = "hive"
        database = "dc"
    elif sid == "cloud":
        host = "10.200.100.126"
        port = 10000
        username = "hive"
        database = "dc"
    try:
        conn_hive = hive.Connection(host=host, port=port, username=username, database=database)
    except Exception as e:
        print(e)
    return conn_hive
