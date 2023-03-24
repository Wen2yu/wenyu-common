# -*- coding:utf-8 -*-

# Name: log_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2020-01-06 9:47


import logging

_log_level_switch = {
    'debug': lambda x: x.debug,
    'info': lambda x: x.info,
    'warn': lambda x: x.warn,
    'error': lambda x: x.error,
}


def log(app=None, msg='', level='info'):
    """
    输出日志
    :param app: 所在app(flask启动对象)
    :param msg: 日志内容
    :param level: 日志级别
    :return:
    """
    logger = app.logger if hasattr(app, 'logger') else logging
    return _log_level_switch[level.lower()](logger)(msg)
