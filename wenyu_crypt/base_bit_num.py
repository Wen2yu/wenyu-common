# -*- coding:utf-8 -*-

# Name: BaseNum
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2019-12-24 8:42

import string


class BaseBitNum(object):

    def __init__(self, num_bit=10):
        """

        :param num_bit: 数位 2/8/10/16/32...
        """
        self._num_bit = num_bit

    @property
    def num_bit(self):
        return self._num_bit

    @property
    def bit_chars(self):
        return self.get_bit_chars()

    def get_bit_chars(self):
        return [ch for ch in string.printable[:self.num_bit]]


BINARY = BaseBitNum(num_bit=2)
OCTAL = BaseBitNum(num_bit=8)
NUM = BaseBitNum(num_bit=10)
HEX = BaseBitNum(num_bit=16)
BASE32 = BaseBitNum(num_bit=32)
BASE64 = BaseBitNum(num_bit=64)

