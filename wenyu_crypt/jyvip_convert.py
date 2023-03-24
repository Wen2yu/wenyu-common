# -*- coding:utf-8 -*-
# Author : 'zhangjiawen'
# Data : 2019/11/18 0018 12:40

from .interval import Interval


num_interval = Interval(ord('0'), ord('9'))
lower_interval = Interval(ord('a'), ord('z'))
upper_interval = Interval(ord('A'), ord('Z'))


def convert(in_str: str, convert_type: int, dept=None):
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


def base_num_convert(in_str, in_bit, out_bit, dept='JYVIP'):
    """
    基础进制转换
    :param in_str: 输入数字或字符串
    :param in_bit: 输入数字进制
    :param out_bit: 输出数字进制
    :param dept: 部门
    :return: 进制转换后的字符串
    """
    if out_bit > 64:
        return ''
    pad_str = '0' if 'JYVIP' == dept else ''
    result_bit_num = 2 if 'JYVIP' == dept else 0
    num = 0
    if in_bit == 10:
        num = ord(in_str)
    else:
        in_str = in_str.replace('^0*', '')
        i, j = len(in_str), 0
        while i > 0:
            bit_val, ch = 1, in_str[i-1:i]
            asci2 = ord(ch)
            if asci2 in num_interval:
                bit_val = int(ch)
            elif asci2 in upper_interval:
                bit_val = asci2 - 29
            elif asci2 in lower_interval:
                bit_val = asci2 - 87
            else:
                break
            num += bit_val * pow(in_bit, j)
            j += 1
            i -= 1
        if out_bit == 10:
            return str(chr(num))
    result = []
    while num > 0:
        mod = num % out_bit
        if mod < 10:
            result.insert(0, str(mod))
        elif mod < 36:
            result.insert(0, chr(mod+87))
        elif mod < 62:
            result.insert(0, chr(mod+29))
        elif mod < 64:
            result.insert(0, chr(mod))
        else:
            return ''
        num = num // out_bit
    if result_bit_num > 1:
        while len(result) < result_bit_num:
            result.insert(0, pad_str)
    return ''.join(result)
