# -*- coding:utf-8 -*-

# Name: bit_num
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2019-12-24 9:27

from functools import reduce

from .base_bit_num import BaseBitNum, NUM


class BitNum(object):
    def __init__(self, val, bit=NUM):
        """
        初始化
        :param val: 值
        :param bit: 数位
        """
        self._val = val
        self._bit = bit.num_bit
        self._bit_chars = bit.bit_chars

    def convert_to_bit(self, aim_bit: BaseBitNum):
        """
        进制转换
        :param aim_bit: 目标数位
        :return:
        """
        d, b, result = self.val_num, aim_bit.num_bit, []
        while d:
            m, d = d % b, d // b
            result.append(aim_bit.bit_chars[m])
        if result:
            result.reverse()
        return ''.join(result)

    @property
    def val_num(self) -> int:
        """
        对应的十进制数值
        :return:
        """
        if self._bit == NUM.num_bit and type(self._val) == int:
            return self._val
        else:
            return reduce(
                lambda x, y: x * self._bit + y,
                [v for v in map(lambda x: self._bit_chars.index(x), self._val)]
            )

    def get_bit_char(self, m):
        """
        数值转字符
        :param m: 数值
        :return:
        """
        return self._bit_chars[m] if m < len(self._bit_chars) else ''

    def get_bit_val(self, ch):
        """
        字符转数值
        :param ch: 字符
        :return:
        """
        return self._bit_chars.index(ch) if ch in self._bit_chars else -1
