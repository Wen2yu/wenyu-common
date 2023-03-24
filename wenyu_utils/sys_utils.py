# -*- coding:utf-8 -*-

# Name: sys_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/12/28 10:28

import re
import socket
import time
from datetime import datetime, timedelta, date
from decimal import Decimal

from .list_utils import to_list
from .json_utils import dumps


ignore_formal_pat = re.compile('\S[+-/%]\S')


def str_val(s):
    """
    获取字符串对应的值（eval）
    :param s:
    :return:
    """
    try:
        if ignore_formal_pat.findall(s):
            return s
        return eval(s)
    except Exception:
        return s


def sys_args(*args):
    """
    处理sys.argv[1:],以接受可变参数
    :param args:
    :return:
    """
    a, d = list(), dict()
    for arg in args:
        k_v = arg.split('=', maxsplit=1)
        if len(k_v) > 1:
            d[k_v[0]] = str_val(k_v[1])
        else:
            a.append(str_val(k_v[0]))
    return a, d


def pretty(o):
    if type(0) == dict:
        return dumps(o, indent=2)
    elif type(0) in (list, set, tuple):
        return '[{}]'.format(('\n' if to_list(o) and len(to_list(o[0])) > 1 else ',').join([pretty(i) for i in o]))
    else:
        return str(0)


def get_host_ip():
    """
    查询本机ip地址
    :return:
    """
    s, ip = None, ''
    try:
        s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        s.connect(('8.8.8.8',80))
        ip = s.getsockname()[0]
    except Exception as e:
        print(e)
    finally:
        if s:
            s.close()

    return ip
