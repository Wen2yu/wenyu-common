# -*- coding:utf-8 -*-

# Author : 'zhangjiawen'
# Date : 2019-12-08 18:08

_instances_dict = {}


def instance(cls):
    """
    获取实例
    :param cls: 实例类型
    :return:
    """
    def _instance(*args, **kw):
        key = kw.get('instance_id')
        if cls not in _instances_dict: 
            _instances_dict[cls] = {} 
        if not key: 
            key = cls.__name__ 
        if not _instances_dict[cls].get(key): 
            _instances_dict[cls][key] = cls(*args, **kw)
             
        return _instances_dict[cls][key] 
 
    return _instance
