# -*- coding:utf-8 -*-

# Name: __init__.py
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/11 12:41

import copy

from .base_db import BaseDbFactory, wraps, handle_param, gen_sql_with_params, parse_data
from .cx_orcl import OrclFactory
from .es import EsFactory
from .py_mysql import MysqlFactory
from .py_hive import HiveFactory
from .impala import ImpalaFactory
from .sqlite import SqliteFactory


db_factory_dict = dict()


def db_factory(db_type: str = None):
    """
    获取db_type 对应的数据库Factory
    :param db_type: 数据库类型
    :return:
    """
    if not db_factory_dict:
        for cls in BaseDbFactory.__subclasses__():
            db_factory_dict[cls.__name__.replace('Factory', '')] = cls
    db_factory_cls = db_factory_dict.get(db_type)
    if db_factory_cls:
        return db_factory_cls()
    else:
        print(f'[jydata-common]: db_factory not support {db_type} database')
        return None


def check_db_factory(def_result=None):
    def wrapper(func):

        @wraps(func)
        def inner(*args, **kw):
            db_type = kw.pop('db_type', None) or args[0]
            factory = db_factory(db_type)
            if factory:
                kw['db_factory'] = factory
                return func(*args, **kw)
            else:
                return def_result
        return inner

    return wrapper


@check_db_factory()
def get_db(db_type: str = None, key: str = None, **kw):
    """
    get 数据库实例
    :param db_type: 数据库类型
    :param key: 数据库实例标识key
    :param kw: 数据库实例初始化参数
    :return: 数据库实例
    """
    return kw.pop('db_factory').get_db(key, **kw)


@check_db_factory(def_result=dict())
def show_db_confs(db_type: str = None, **kw) -> dict:
    """
    查看 数据库配置信息
    :param db_type: 数据库类型
    :return:
    """
    db_confs = dict()
    for key, db_conf in kw.pop('db_factory').db_confs().items():
        db_confs[key] = copy.deepcopy(db_conf)
        if db_conf.get('passwd'):
            db_confs[key].pop('passwd')
    return db_confs

