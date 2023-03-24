# -*- coding:utf-8 -*-

# Name: func_ret_code
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/7/23 9:49

from enum import Enum
from jy_entity import BaseEnumItem as CodeItem


class FuncRetCode(Enum):
    NO_HANDLE = CodeItem(-4, 'Did not do any Handle')
    FAILED = CodeItem(-3, 'Handle failed')
    ERROR = CodeItem(-2, 'Error occurred')
    EXCEPT = CodeItem(-1, 'Exception')
    NO_DATA = CodeItem(0, 'No data return')
    SUCCESS = CodeItem(1, 'Successful')

