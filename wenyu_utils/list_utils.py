# -*- coding:utf-8 -*-

# Author : 'zhangjiawen'
# Date : 2019/11/18 0018 12:40

import re

range_pat = re.compile(r'(\d*):(\d*):?(\d*)')
attr_kv_pat = re.compile(r'(\S+)=(.+)')


def first(items, condition=None, def_val=None):
    """
    获取对象集合中符合条件的第一个对象
    :param items: 对象集合
    :param condition: 过滤条件
    :param def_val: 默认返回值
    :return:
    """
    if len(items) > 0:
        if condition:
            for i in items:
                if condition(i):
                    return i
        else:
            return items[0]
    return def_val


def to_list(a):
    """

    :param a:
    :return:
    """
    if type(a) == list:
        return a
    if type(a) in (tuple, set):
        return list(a)
    else:
        return [a]


def to_set(a):
    """
    list/tuple转set
    :param a:
    :return:
    """
    try:
        return set(a)
    except Exception:
        return a


def union(a, b, distinct=True):
    """
    list/tuple/set 求并集
    :param a: 第一个集合
    :param b: 第二个集合
    :param distinct: 结果是否去重
    :return:
    """
    if type(a) == set:
        return a.union(b) if type(b) in [set, list, tuple] else a.add(b)
    elif type(a) == list:
        if distinct:
            bd = list(b) if type(b) in [set, list, tuple] else [b]
            return [i for i in a if not first(bd, lambda j: type(j) == type(i) and (
                getattr(j, 'id', None) == i.id if hasattr(i, 'id') else (
                    j.get('id', None) == i.get('id', 0) if type(i) == dict else to_set(j) == to_set(i)
                )
            ))] + bd
        else:
            return a + list(b) if type(b) in [set, list, tuple] else a.append(b)
    elif type(a) == tuple:
        return tuple(union(list(a), b, distinct=distinct))
    else:
        return [a, b]


def get_item_by_key(a, k: str):
    """
    根据k值获取列表元素集
    :param a:
    :param k:
    :return:
    """
    try:
        e_k = eval(k)
    except Exception:
        e_k = k
    if k in a:
        return k
    elif e_k in a:
        return e_k
    elif type(e_k) in (int, float):
        return nrd(a, int(e_k))
    elif range_pat.fullmatch(k):
        b, e, step = range_pat.fullmatch(k).groups()
        return a[eval(b) if b else 0:eval(e) if e else len(a):eval(step) if step else 1]
    elif attr_kv_pat.fullmatch(k):
        k, v = attr_kv_pat.fullmatch(k).groups()
        try:
            e_v = eval(v)
        except Exception:
            e_v = v
        return first(a, lambda i: (type(i) == dict and (i.get(k) == v or (e_v and i.get(k) == e_v))) or (
                hasattr(i, k) and (getattr(i, k) == v or (e_v and getattr(i, k) == e_v))))
    else:
        return first(a, lambda i: (type(i) == dict and i.get('id') == e_k) or (getattr(i, 'id', None) == e_k))


def nrd(items, n, last_replace=False):
    """
    返回第n个元素
    :param items:
    :param n: 第n个
    :param last_replace: 如果元素个数不足是否用最后一个代替，如果否，元素不足时返回None
    :return:
    """
    if len(items) >= n:
        return items[n-1]
    else:
        return items[-1] if items and last_replace else None
