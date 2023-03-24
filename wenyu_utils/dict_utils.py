# -*- coding:utf-8 -*-

# Name: dict_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/7/26 13:21

import copy
from .list_utils import union


def deep_update(d1: dict, d2: dict, **kw):
    """
    深度更新dict，如果源字典(d1)key对应的值是集合类对象，则如果d2存在相同的key，则合并d1[v]和d2[v]，而不是直接用d2[v]替换d1[v]
    :param d1: 源dict
    :param d2:
    :param distinct: 合并集合对象是否去重，默认不去重
    :param left: 末端是否左保留
    :return: 更新后的源dict, 即d1
    """
    for k, v in d2.items():
        if type(d1.get(k, None)) == type(v) == dict:
            d1[k] = deep_update(d1[k], v, **kw)
        elif kw.get('merge_list', True) and type(d1.get(k, None)) in (list, tuple, set) and type(v) in (list, tuple, set):
            d1[k] = union(d1[k], v, distinct=kw.get('distinct', False))
        elif kw.get('merge_obj2list', True) and d1.get(k) and type(v) in (list, tuple, set):
            d1[k] = union([d1[k]], v, distinct=kw.get('distinct', False))
        else:
            if not (kw.get('left', False) and d1.get(k)):
                d1[k] = v
    return d1


def left_union(d1: dict, d2: dict):
    """
    左保全合并dict
    :param d1: 源dict
    :param d2:
    :return: 更新后的源dict, 即d1
    """
    for k, v in d2.items():
        if not d1.get(k):
            d1[k] = v
    return d1


def deep_get(d: dict, **kw):
    """
    根据路径取值
    :param d:
    :param path:
    :param split:
    :return:
    """
    v = d
    def_val = kw.get('default_val')
    if kw.get('path'):
        for k in kw['path'].split(kw.get('split', '.')):
            if type(v) == dict:
                v = v.get(k, def_val)
            else:
                break
    if kw.get('values', 0):
        v = list(v.values())
    if v == d or not v:
        v = def_val
    return v


def deep_pop(d: dict, **kw):
    """
    根据路径pop键值对
    :param d:
    :param path:
    :param split:
    :return:
    """
    v = d
    def_val = kw.get('default_val')
    if kw.get('path'):
        ks = kw['path'].split(kw.get('split', '.'))
        for k in kw['path'].split(kw.get('split', '.'))[:-1]:
            v = v.get(k, def_val)
        v = v.pop(ks[-1], def_val) if v and type(v) == dict else None
    if v == d or not v:
        v = def_val
    return v

