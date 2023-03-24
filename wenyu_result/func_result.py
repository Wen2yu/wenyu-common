# -*- coding:utf-8 -*-

# Name: func_result
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/7/23 10:01

from jy_code.func_ret_code import FuncRetCode
from jy_utils.operation_utils import add


class FuncResult:

    def __init__(self, fun_ret_code: FuncRetCode, data=None, desc=None, msg: dict = dict()):
        code_item = fun_ret_code
        if desc is not None:
            code_item.desc = desc
        self._code = code_item
        self.data = data
        self.msg = msg

    def set_data(self, data):
        self.data = data

    def set_msg(self, msg: dict):
        self.msg = msg

    def add_msg(self, key, msg):
        if self.msg.get(key):
            self.msg[key] = add(self.msg[key], msg)
        else:
            self.msg[key] = msg

    @property
    def code(self):
        return self._code.value

    @property
    def code_val(self):
        return self.code.code

    @property
    def code_desc(self):
        return self.code.desc

    @property
    def is_success(self):
        return self.code_val == FuncRetCode.SUCCESS.value.code

    @property
    def no_handle(self):
        return self.code_val == FuncRetCode.NO_HANDLE.value.code

    @property
    def no_data(self):
        return self.code_val == FuncRetCode.NO_DATA.value.code

    def __repr__(self):
        return f'<FuncResult[{self.code}, {self.msg}, {self.data}]>'
