# -*- coding:utf-8 -*-

# Name: obj_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/8/3 13:19

import re


class_dict = dict()


def name2class(name: str):
    """
    根据type_name获取type
    :param name: type_name
    """
    if not name:
        return None
    if class_dict.get(name):
        return class_dict[name]
    ss = name.split('.')
    if globals().get(ss[-1]):
        return globals()[ss[-1]]
    if len(ss) > 1:
        package = __import__(ss[0])
        for p_name in ss[1:-1]:
            package = getattr(package, p_name)
        return getattr(package, ss[-1])
    return eval('name')


def serializable_key(key, add_cls=False, cls_k='class'):
    """
    序列化key
    :param key:
    :param add_cls:
    :param cls_k:
    :return:
    """
    if type(key) in [str, int, float, bool, None]:
        return key
    elif type(key) in [list, set, tuple]:
        return str(key)
    else:
        return str(obj2dict(key, add_cls=add_cls, cls_k=cls_k))


def obj2dict(o, add_cls=False, cls_k='class'):
    """
    对象转dict
    :param o: 待转对象
    :param add_cls: 添加类型
    :param cls_k: 结果中类型字段对应的k值，仅在默认('cls')与代转对象属性冲突时使用
    :return:
    """
    class_name_pat = re.compile(r"<class '(.+)'>")
    if not hasattr(o, '__dict__'):
        return str(o) if type(o) in [list, set, tuple] else o
    result: dict = dict()
    if add_cls:
        result[cls_k] = class_name_pat.findall(str(type(o)))[0]
    d = {
        serializable_key(
            k, add_cls=add_cls, cls_k=cls_k
        ): obj2dict(v, add_cls=add_cls, cls_k=cls_k) if hasattr(v, '__dict__') else (
            {
                serializable_key(
                    m, add_cls=add_cls, cls_k=cls_k
                ): obj2dict(n, add_cls=add_cls, cls_k=cls_k) for m, n in v.items()
            } if type(v) == dict else v
        ) for k, v in o.__dict__.items()
    }
    result.update(d)
    return result


def dict2obj(d: dict, cls_k='class', copy=False, fields_dict=None):
    """
    dict 转对象
    :param d: 待转dict
    :param cls_k: 类型字段对应的k值
    :param copy: 是否全复制对象
    :param fields_dict: 参与创建对象的字段列表{cls: {dict_key: obj_param_name}}}
    :return:
    """
    o = d
    if type(d) == dict:
        cls = name2class(d.pop(cls_k, None))
        if cls:
            if copy:
                o = cls()
                for k, v in d.items():
                    setattr(o, k, dict2obj(v, fields_dict=fields_dict))
            else:
                if fields_dict.get(cls):
                    if type(fields_dict[cls]) == dict:
                        d = {fields_dict[cls][k]: v for k, v in d.items() if k in fields_dict[cls]}
                    elif fields_dict[cls] == '___copy___':
                        d[cls_k] = cls
                        return dict2obj(d, copy=True)
                kw = {
                    eval(k): dict2obj(v, fields_dict=fields_dict) if type(v) == dict else v\
                    for k, v in d.items() if not getattr(cls, '__fields__', None) or k in cls.__fields__
                }
                o = cls(**kw)
    return o


def none(obj, v):
    """
    如果对象为None则转为默认值
    :param obj: 待转对象
    :param v: 默认值
    :return:
    """
    return obj if obj is not None else v


def next_val(obj, sort=False, reverse=False, limit=10000, def_val=None, params=None):
    """
    下一个值
    :param obj:
    :param sort:
    :param reverse:
    :param limit:
    :param def_val:
    :param params:
    :return:
    """
    if type(obj) in [list, set]:
        if sort:
            obj = sorted(obj, reverse=reverse)
        limit = len(obj)
    elif type(obj) == dict:
        if sort:
            obj = {i: obj[i] for i in sorted(obj)}
        limit = len(obj)
    for i in range(limit):
        if type(obj) in [list, set, tuple]:
            yield obj[i]
        elif type(obj) == dict:
            yield obj.pop(list(obj.keys()).pop())
        elif hasattr(obj, '__next__'):
            yield next(obj, def_val)
        elif callable(obj):
            g_params = next_val(params) if params else (i for i in range(limit))
            yield obj(next(g_params))
        else:
            yield obj
