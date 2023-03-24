# -*- coding:utf-8 -*-

# Name: base_log
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/7/23 9:56

from .base_model import BaseModel


class BaseLog(BaseModel):
    """
    日志类基类
    """

    def save(self):
        pass

