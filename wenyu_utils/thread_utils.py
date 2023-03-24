# -*- coding:utf-8 -*-

# Name: thread_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/8/3 9:29

from functools import wraps


def lock_obj(def_return=1, **kw):
    """

    :param def_return: 默认返回值
    :return:
    """
    def wrapper(func):
        @wraps(func)
        def inner(self, *args, **kw):
            if self.lock:
                self.lock.acquire(blocking=kw.get('blocking', self.blocking), timeout=kw.get('timeout', self.timeout))
            return func(self, *args, **kw) or def_return

        return inner

    return wrapper


def release_obj(def_return=1):
    """

    :param def_return: 默认返回值
    :rtype: object
    :return:
    """
    def wrapper(func):
        @wraps(func)
        def inner(self, *args, **kw):
            if self.lock and self.lock.locked():
                self.lock.release()
            return func(self, *args, **kw) or def_return

        return inner

    return wrapper
