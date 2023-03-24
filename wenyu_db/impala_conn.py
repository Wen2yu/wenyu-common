# -*- coding: utf-8 -*-

# Name: impala_conn
# Product_name: PyCharm
# Description:
# Author: 'caoyitao'
# Date: 2021-06-11 17:28:56

# 导入模块
from impala.dbapi import connect


# 定义函数
def __impala_conn__(sid):
    """
    //获取impala连接；
    """
    if sid == "cloud":
        host = "10.200.100.54"
        port = 21050
    elif sid == "algorithm":
        host = "10.1.5.12"
        port = 21050
    try:
        conn_impala = connect(host=host, port=port)
    except Exception as e:
        print(e)
    return conn_impala
