# -*- coding:utf-8 -*-
# Author : 'zhangjiawen'
# Date : 2019/11/18 0018 12:40


from .base_bit_num import *
from .jyvip_convert import convert as jyvip_convert


def cript(in_str: str, convert_type: int, dept=None):
    """
    加解密（数位转换）
    :param in_str: 待加（解）密字符串
    :param convert_type: 1:解密， 0:加密
    :param dept:
    :return:
    """
    result = []
    step = 2 if convert_type else 1
    for i in range(0, len(in_str), step):
        if convert_type:
            result.append(base_num_convert(in_str[i: i + step], 32, 10, dept))
        else:
            result.append(base_num_convert(in_str[i: i + step], 10, 32, dept))
    return ''.join(result)


