# -*- coding:utf-8 -*-

# Name: base_obj
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/7/23 9:51

from threading import Lock

from jy_utils.date_utils import convert_val_to_datetime


class BaseObj(object):

    datetime_cols = []

    def __init__(self, *args, **kwargs):
        if kwargs:
            for key in kwargs.keys():
                if hasattr(self, key):
                    setattr(self, key,
                            convert_val_to_datetime(
                                kwargs.get(key), fmt=kwargs.get('fmt')) if key in self.datetime_cols else kwargs.get(key))

    def __repr__(self):
        return str(self.__dict__)

    def __str__(self):
        return str(self.__dict__)


class SingleObj(BaseObj):
    _instance_lock = Lock()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            with SingleObj._instance_lock:
                if not hasattr(cls, '_instance'):
                    cls._instance = super().__new__(cls)
        return cls._instance


class BaseFactory(SingleObj):
    pass
