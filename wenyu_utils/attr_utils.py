# -*- coding: utf-8 -*-

# Author : 'zhangjiawen'


def set_attrs(o, **kwargs):
    """
    设置对象属性，用于将dict赋值给对象属性
    :param o: 待设置属性对象
    :param kwargs: 需要设置的属性键值对
    :return: None
    """
    for k, v in kwargs.items():
        if hasattr(o, k):
            setattr(o, k, v)
