# -*- coding:utf-8 -*-

# Name: code_item
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/7/23 9:50

from jy_entity import BaseObj


class CodeItem(BaseObj):

    def __init__(self, code, desc, **kwargs):
        super().__init__(**kwargs)
        self.code = code
        self.desc = desc

