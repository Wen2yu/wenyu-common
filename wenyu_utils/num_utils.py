# -*- coding: utf-8 -*-

# Author : 'zhangjiawen'


MAX_INT = 2**32-1  # 最大整数


def is_number(val):
    """
    判断给定的值是否为数字
    :param val:
    :return:
    """
    if type(val) in [int, float]:
        return True
    if type(val) == str:
        try:
            float(val)
            return True
        except ValueError:
            return False
    return False


def format_number(number, min_len=2, max_len=0, fill_num=0, fill_loc=1, sub_loc=1):
    """
    格式化时间元素数字
    :param numbers: 带格式化数字组
    :param min_len: 最小长度
    :param max_len: 最大长度
    :param fill_num: 不足最小长度时填充的数字
    :param fill_loc: 填充位置：1：左填充；-1：右填充
    :param sub_loc: 超出长度截取位置：1：左截取；-1：右截取
    :return:
    """
    if not is_number(number):
        return number
    ns = str(number)[:sub_loc * max_len] if max_len and sub_loc > 0 else str(number)[sub_loc * max_len:]
    return str(fill_num) * (min_len - len(ns)) + ns if fill_loc > 0 else ns + str(fill_num) * (min_len - len(ns))


def int2list(num: int, divide_num: int = 10, reverse=False):
    """
    int 根据除数拆分成list
    :param num: 待拆分数
    :param divide_num: 除数
    :param reverse: 是否反转
    :return:
    """
    result = []
    if not divide_num:
        divide_num = 10
    while num >= divide_num:
        result.insert(0, num % divide_num)
        num = int(num / divide_num)
    result.insert(0, num % divide_num)
    if reverse:
        result.reverse()
    return result


# print(int2list(27, divide_num=26, reverse=True))

