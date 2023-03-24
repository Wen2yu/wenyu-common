# -*- coding:utf-8 -*-

# Name: json_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Data : 2019-12-31 16:50

import json
from jy_code.json_encoder import DataEncoder

from .file_utils import read_str


def dumps(item, **kw):
    """
    对象转json字符串
    :param item: 对象
    :return:
    """
    return json.dumps(item, cls=DataEncoder, **kw)


def loads(json_str, encoding='utf8', **kw):
    """
    json字符串转对象
    :param json_str:
    :param encoding:
    :param kw:
    :return:
    """
    return json.loads(json_str, encoding=encoding, **kw)


def load_json_file(path, encoding='utf8', **kw):
    """
    从jsonfile加载对象
    :param path:
    :param encoding:
    :param kw:
    :return:
    """
    try:
        return json.loads(read_str(path, encoding=encoding, mode=kw.pop('mode', 'r')), encoding=encoding, **kw)
    except Exception as e:
        print(e)
    return dict()
