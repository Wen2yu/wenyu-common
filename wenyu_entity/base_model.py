# -*- coding:utf-8 -*-

# Name: base_model
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/7/23 9:54

from datetime import datetime
from decimal import Decimal
from sqlalchemy import types

from .base_obj import BaseObj
from jy_utils.date_utils import convert_val_to_datetime


class SqliteNumeric(types.TypeDecorator):
    impl = types.String

    @property
    def python_type(self):
        pass

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(types.VARCHAR(100))

    def process_bind_param(self, value, dialect):
        return str(value)

    def process_literal_param(self, value, dialect):
        pass

    def process_result_value(self, value, dialect):
        return Decimal(value) if value and str(value).upper() != 'NONE' else Decimal(0)


Numeric = SqliteNumeric


def decode_crm(num):
    char_dict = {'1g': '0', '1h': '1', '1i': '2', '1j': '3', '1k': '4', '1l': '5', '1m': '6', '1n': '7', '1o': '8',
                 '1p': '9', '2o': 'X', '3o': 'X'}
    return ''.join([char_dict.get(num[i*2:(i+1)*2], '') for i in range(len(num) // 2)]) if type(num) == str else ''


class BaseModel(BaseObj):

    col_names = []
    datetime_cols = []
    decode_cols = []
    ts = 0

    type_default_val_switch = {
        int: 0,
        str: '',
        None: '',
        datetime: datetime(1970, 1, 1),
        Numeric: 0
    }

    def __init__(self, *args, item=None, **kwargs):
        if args:
            for i in range(len(args)):
                setattr(self, self.col_names[i],
                        convert_val_to_datetime(args[i]) if self.col_names[i] in self.datetime_cols else (
                            str(args[i]) if type(args[i]) in [list, dict, set] else args[i]
                        ))
        if item:
            for col_name in self.col_names:
                if hasattr(item, col_name):
                    val = getattr(item, col_name, '')
                    val = decode_crm(val) if col_name in self.decode_cols and not kwargs.get('no_decrypt') else val
                    setattr(self, col_name,
                            convert_val_to_datetime(val) if col_name in self.datetime_cols else (
                            str(val) if type(val) in [list, dict, set] else val
                        ))
        if kwargs:
            for (k, v) in kwargs.items():
                if type(v) in [list, dict, set]:
                    kwargs[k] = str(v)
                if type(v) == bool:
                    kwargs[k] = int(v)
            super().__init__(**kwargs)
        self.ts = kwargs.get('ts')

