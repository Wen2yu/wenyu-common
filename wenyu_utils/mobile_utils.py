# -*- coding:utf-8 -*-

# Name: mobile_utils
# Product_name: PyCharm
# Description:
# Author : zhangjiawen
# Date : 2021-02-20 12:53

import random


mobile_num_pries = [
    134, 135, 136, 137, 138, 139, 147, 148, 150, 151, 152, 157, 158, 159, 172, 178,
    182, 183, 184, 187, 188, 195, 197, 198, 165, 170,
    130, 131, 132, 145, 146, 155, 156, 166, 171, 175, 176, 185, 186, 196,
    133, 149, 153, 173, 177, 180, 181, 189, 190, 191, 193, 199,
]


def random_mobile(pries=None):
    """
    随机生成1个手机号
    :param pries: 指定前缀list
    :return: 1个手机号字符串
    """
    if not pries:
        pries = mobile_num_pries
    pre = random.choice(pries)
    return str(pre * 100000000000 + random.randint(1, 99999999999))[:11]


def random_mobiles(n, pries=None):
    """
    随机生成n个手机号
    :param n: 生成手机号数量
    :param pries: 指定前缀list
    :return: 包含n个手机号的生成器
    """
    index = 0
    while index < n:
        yield random_mobile(pries)
        index += 1
