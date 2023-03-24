# -*- coding:utf-8 -*-
# Author : 'zhangjiawen'
# Data : 2019/11/23 0023 11:25

from enum import Enum


class InsertType(Enum):
    TEMP = (-1, 'temp')
    APPEND = (1, 'append')
