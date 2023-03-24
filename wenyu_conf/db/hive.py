# -*- coding:utf-8 -*-

# Name: hive
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/17 16:53

hive_infos = {
    'dc': {
        'host': '10.10.11.228',
        'port': 10000,
        'username': 'hive',
        'database': 'dc',
    },
    'cloud': {
        'host': '10.200.100.126',
        'port': 10000,
        'username': 'impala',
        'hdfs_key': 'cloud',
        'hdfs_path': '/user/hive/warehouse/'
    },
}


def get_hive_conf(key: str) -> dict:
    return hive_infos.get(key, dict())


