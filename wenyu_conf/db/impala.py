# -*- coding:utf-8 -*-

# Name: impala
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/17 16:59

impala_infos = {
    'cloud': {
        'host': '10.200.100.54',
        'port': 21050,
        'user': 'work',
        'hive_key': 'cloud',
    },
    'algorithm': {
        'host': '10.1.5.12',
        'port': 21050,
        'hive_key': 'dc',
    }
}


def get_impala_conf(key: str) -> dict:
    return impala_infos.get(key, dict())
