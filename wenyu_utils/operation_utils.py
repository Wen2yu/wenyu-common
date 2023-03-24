# -*- coding:utf-8 -*-

# Name: operation_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2019-12-27 9:40

import traceback


add_switch = {
    list: lambda x1, x2: _add_list_obj(x1, x2),
    dict: lambda x1, x2: _add_dict_obj(x1, x2),
    None: lambda x1, x2: x2,
}


def add(o1, o2):
    """
    追加/合并对象到list/dict
    :param o1:
    :param o2:
    :return:
    """
    func = add_switch.get(type(o1))
    if func:
        return func(o1, o2)
    else:
        try:
            return o1 + o2
        except Exception as e:
            print('the type %s not support add operation, and the reason is %s' % (type(o1), e))
            traceback.extract_stack()
            return o1


def _add_list_obj(o1: list, o2):
    if type(o2) == list:
        o1.extend(o2)
    else:
        o1.append(o2)
    return o1


def _add_dict_obj(o1: dict, o2):
    if type(o2) == dict:
        for k in o2:
            o1 = _add_dict_key_val(o1, k, o2[k])
    else:
        key, value = getattr(o2, 'key', None), getattr(o2, 'value', o2)
        o1 = _add_dict_key_val(o1, key, value)
    return o1


def _add_dict_key_val(o1: dict, key, val):
    if o1.get(key):
        o1[key] = add(o1[key], val)
    else:
        o1[key] = val
    return o1
