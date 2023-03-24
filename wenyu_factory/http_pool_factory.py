# -*- coding: utf-8 -*-

# Author : 'zhangjiawen'

from urllib3 import HTTPConnectionPool, HTTPSConnectionPool


http_pool_dict = dict()


def get_http_pool(host, protocol='http', proxy_url=None, **kwargs) -> HTTPConnectionPool:
    """
    获取http连接池
    :param host: 请求地址
    :param protocol: 请求协议
    :param proxy_url: 代理
    :param kwargs:
    :return:
    """
    key = _gen_key(host, protocol, proxy_url)
    if not http_pool_dict.get(key):
        if protocol == 'https':
            http_pool_dict[key] = HTTPSConnectionPool(host, _proxy=proxy_url, **kwargs)
        else:
            http_pool_dict[key] = HTTPConnectionPool(host, _proxy=proxy_url, **kwargs)
    return http_pool_dict[key]


def remove_http_pool(host, protocol='http', proxy_url=None):
    http_pool_dict.pop(_gen_key(host, protocol, proxy_url))


def _gen_key(*args):
    return '_'.join(args)
