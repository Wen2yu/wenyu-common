# -*- coding:utf-8 -*-

# Name: es
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2022/4/27 17:33


es_infos = {
    'aliyun': {
        'hosts': ['es-cn-i7m2gakrb0011navh.elasticsearch.aliyuncs.com:9200'],
        'user': 'elastic',
        'passwd': 'SX^RA1*nCjp8xU3',
    },
    'algorithm': {
        'host': '10.1.5.12',
        'port': 21050,
        'hive_key': 'dc',
    }
}


def get_es_conf(key: str) -> dict:
    return es_infos.get(key, dict())
