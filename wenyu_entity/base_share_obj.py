# -*- coding:utf-8 -*-

# Name: base_share_obj
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/8/3 9:43

import time
from datetime import datetime
from functools import wraps
from threading import Lock, RLock

from jy_utils.list_utils import first
from jy_utils.obj_utils import next_val
from jy_utils.str_utils import str2values
from jy_utils.thread_utils import lock_obj, release_obj
from .base_obj import BaseObj, BaseFactory


def deparse(wait_rules: str, split=''):
    if wait_rules:
        retry_limits, wait_time, wait_time_out = str2values(wait_rules, split=split)
        return next_val(retry_limits), next_val(wait_time), next_val(wait_time_out)
    else:
        return 0, 0, 0


def retry(wait_rule='5:3:15', split='', def_result=None):
    """
    重试
    :param wait_rule:
    :param split:
    :param def_result:
    :return:
    """
    def wrapper(func):

        @wraps(func)
        def inner(self, *args, **kw):
            result, idx, ts = def_result, 0, datetime.now().timestamp()
            retry_limits_g, wait_time_g, wait_time_out_g = deparse(wait_rule, split=split)
            while result == def_result and (
                    next(retry_limits_g) < 0 or idx < next(retry_limits_g)
            ) and (next(wait_time_out_g) < 0 or datetime.now().timestamp() - ts < next(wait_time_out_g)):
                result = func(self, *args, **kw)
                if result == def_result:
                    time.sleep(next(wait_time_g))
                    idx += 1
            return result

        return inner
    return wrapper


class BaseShareObj(BaseObj):
    is_busying = False

    def __init__(self, *args, lock_type='', blocking=True, timeout=3000, **kwargs):
        super().__init__(*args, **kwargs)
        if lock_type == 'R':
            self.lock = RLock()
        else:
            self.lock = Lock()
        self.blocking = blocking
        self.timeout = timeout

    @lock_obj
    def busying(self):
        self.is_busying = True

    @release_obj
    def freeing(self):
        self.is_busying = False


class BaseShareObjFactory(BaseFactory):
    instance_dict = dict()  # 实例集合
    instance_cls = BaseShareObj  # 实例类型

    def __init__(self, single=False, instances_size=-1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.single = single
        self.instances_size = instances_size

    def get_instance(self, key: str = None, **kw):
        """
        获取实例
        :param key: 实例标识
        :param kw: 实例创建创建参数
        :return:
        """
        key = self.gen_instance_key(key, **kw)
        instances = self.instance_dict.get(key, [])
        while not instances or (self.instances_size > 0 and len(instances) < self.instances_size):
            instance = self._create_instance(key, **kw)
            if self.single:
                return instance
            instances = self.instance_dict.get(key, [])
        return self.choose_freeing_instance(key, **kw)

    def gen_instance_key(self, key: str = None, **kw):
        """
        生成实例标识
        :param key: 实例标识
        :param kw: 实例创建创建参数
        :return:
        """
        return '' if self.single else f'{key}[{kw}]'

    def gen_attrs(self, key, **kw):
        return kw

    def _create_instance(self, key, **kw):
        instance = self.instance_cls(**kw)
        if self.single:
            self.instance_dict[key] = instance
        else:
            if not self.instance_dict.get(key):
                self.instance_dict[key] = []
            self.instance_dict[key].append(instance)
        return instance

    @retry(wait_rule='5:3:15', split=':')
    def choose_freeing_instance(self, key, **kw):
        freeing_instance = first(self.instance_dict[key], lambda i: not i.is_busying)
        if not freeing_instance and (
                self.instances_size <= 0 or len(self.instance_dict.get(key, [])) < self.instances_size):
            self._create_instance(key, **kw)
        result = freeing_instance or first(self.instance_dict[key], lambda i: not i.is_busying)
        if result:
            result.busying()
        return result
