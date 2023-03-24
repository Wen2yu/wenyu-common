# -*- coding:utf-8 -*-

# Author : 'zhangjiawen'
# Date : 2019/11/18 0018 12:40

from enum import Enum


class ConditionType(Enum):
    NOT_EQUAL = 0
    EQUAL = 1
    IS = 2
    LIKE = 3
    IN = 10
    INTERVAL_CLOSED = 11
    INTERVAL_CLOSED_OPEN = 12
    INTERVAL_OPEN_CLOSED = 13
    INTERVAL_OPEN = 14
    NOT_IN = 20
    OUTSIDE_CLOSED = 21
    OUTSIDE_CLOSED_OPEN = 22
    OUTSIDE_OPEN_CLOSED = 23
    OUTSIDE_OPEN = 24


condition_switch = {
    ConditionType.EQUAL: lambda key, val: ()
}
