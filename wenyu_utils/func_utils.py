# -*- coding:utf-8 -*-

# Name: func_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/7/23 13:40

import sys
import time
import traceback
from enum import Enum
from functools import wraps

from jy_code.func_ret_code import FuncRetCode
from jy_db import get_db, parse_data
from jy_result import FuncResult
from .date_utils import now, now_timestamp
from .dict_utils import deep_update
from .json_utils import dumps
from .obj_utils import next_val
from .str_utils import str2values
from .sys_utils import get_host_ip

FUNC_MAP = {}


class OrderType(Enum):
    DEFAULT = 0
    ORDER = 1
    PARALLEL = 2
    OTHER = 3


func_run_switch = {
    OrderType.DEFAULT: lambda funcs: func_stream(funcs),
    OrderType.ORDER: lambda funcs: func_stream(funcs),
    OrderType.PARALLEL: lambda funcs: func_parallel(funcs)
}


def regist_func(name=None, app=None, level=1, default_params=None, replace=False):
    """
    regist func for later use
    :param name:
    :param app:
    :param level:
    :param default_params:
    :param replace:
    :return:
    """
    app_name = getattr(app, 'name', 'Flask')

    def decorate(f):
        key = '.'.join([app_name, str(level), name if name else f.__name__])
        if not FUNC_MAP.get(key) or replace:
            FUNC_MAP[key] = (f, default_params)
        else:
            regist_func(name, app_name, level+1, default_params, replace)
        return f
    return decorate


def run_task(app, log_model=None):
    """
    执行任务
    :param app: 任务所在app
    :param log_model: 执行日志数据模型
    :return:
    """
    def wrapper(func):

        @wraps(func)
        def inner(*args, **kwargs):
            func_params = dumps(kwargs)
            app.logger.info(f'task[{func.__name__}] begin, params={func_params}')
            log_prop = {
                'app': getattr(app, 'name', str(app)),
                'task_name': func.__name__,
                'super_task': kwargs.get('super_task'),
                'create': now(),
                'params': func_params,
            }
            result = func(*args, **kwargs)
            if type(result) == FuncResult:
                log_prop['result_code'] = result.code_val
                log_prop['result_desc'] = result.code_desc
            if log_model and 'save' in dir(log_model):
                log_model(**log_prop).save()
            else:
                result_msg = str(getattr(result, 'msg', ''))
                get_db('Mysql', 'crawl').save(
                    table_name='jydata_task_log',
                    data=[(
                        kwargs.get('app_name', log_prop['app']), kwargs.pop('func_level', 0), func.__name__, kwargs.get('super_task', ''),
                        '&'.join([f'{k}={v}' for k, v in kwargs.items()]), str(getattr(result, 'code', '')),
                        f'{result_msg[:100]}......{result_msg[-100:]}' if len(result_msg) > 300 else result_msg
                    )],
                    columns=['app_name', 'task_level', 'task', 'super_task', 'params', 'result_code', 'result_msg']
                )
            app.logger.info(f'task[{func.__name__}] end, params={func_params}, result={result}')
            return result

        return inner
    return wrapper


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
            result, idx, ts = def_result, 0, now_timestamp()
            retry_limits_g, wait_time_g, wait_time_out_g = deparse(wait_rule, split=split)
            while result == def_result and (
                    next(retry_limits_g) < 0 or idx < next(retry_limits_g)
            ) and (next(wait_time_out_g) < 0 or now_timestamp() - ts < next(wait_time_out_g)):
                result = func(self, *args, **kw)
                if result == def_result:
                    time.sleep(next(wait_time_g))
                    idx += 1
            return result

        return inner
    return wrapper


def deparse(wait_rules: str, split=''):
    if wait_rules:
        retry_limits, wait_time, wait_time_out = str2values(wait_rules, split=split)
        return next_val(retry_limits), next_val(wait_time), next_val(wait_time_out)
    else:
        return 0, 0, 0


def exception(app=None, except_type=None, msg: tuple = None):

    def wrapper(func):

        @wraps(func)
        def inner(*args, **kw):
            try:
                result = func(*args, **kw)
            except (except_type or Exception) as e:
                traceback.print_exc()
                if app:
                    app.logger.error(f'{func.__name__} has occurred exception: {e}')
                return FuncResult(FuncRetCode.EXCEPT, msg=deep_update(
                    {f'{func.__name__}_error': e}, {msg[0]: msg[1:]} if msg else dict()
                ))
            return result

        return inner
    return wrapper


def save_jydata_script_run_log(**kw):
    """
    保存脚本执行错误信息
    :param kw: host, start_ts, end_ts, script_path, params, msg, belonger, send_freq
    :return:
    """
    kw['host'] = kw.get('host') or get_host_ip()
    columns = ['host', 'start_ts', 'end_ts', 'script_path', 'params', 'msg', 'belonger', 'send_freq']
    try:
        get_db('Mysql', 'jydata_etl').save(
            table_name='jydata_script_run_log', data=[tuple([kw.get(k) for k in columns])], columns=columns
        )
    except Exception as e:
        print(e)
        return 0
    return 1


def resolve_jydata_script_run_log(**kw):
    """
    更新脚本执行错误信息解决状态
    :param kw:
    :return:
    """
    log_id = kw.pop('log_id', 0)
    etl = get_db('Mysql', 'jydata_etl')
    if log_id:
        if log_id > 0:
            log = parse_data(*etl.select(
                'select start_ts, end_ts, params, msg, belonger, send_freq from jydata_script_run_log where id = #_id',
                _id=log_id, include_col_names=1
            ))
            etl.update("""
              update jydata_script_run_log
                set start_ts = #start_ts, end_ts = #end_ts, params = #params, msg = #msg, belonger = #belonger,
                  send_freq = #send_freq, is_solved = #is_solved
              where id = #log_id
            """, log_id=log_id, **deep_update(log, kw))
        else:
            etl.update('update jydata_script_run_log set is_solved = -1 where id = #log_id', log_id=-log_id)
    return log_id


def jy_except(app=None, except_type=None, **kwargs):

    def wrapper(func):

        @wraps(func)
        def inner(*args, **kw):
            result = None
            _kw = {
                'host': kw.pop('host', get_host_ip()),
                'start_ts': int(time.time()),
                'script_path': kw.get('script_path') or kwargs.get('script_path'),
                'params': ' '.join([str(i) for i in args] + [f'{k}={v}' for k, v in kw.items()]),
                'msg': 'success',
                'belonger': kw.get('belonger') or kwargs.get('belonger', 'system'),  ## 归属人
                'send_freq': kw.get('send_freq') or kwargs.get('send_freq', '1d')  ## 发送频次
            }
            log_id = kw.get('log_id') or kwargs.get('log_id')
            try:
                if log_id >= 0:
                    result = func(*args, **kw)
            except (except_type or Exception) as e:
                if app:
                    app.logger.error(f'{func.__name__} has occurred exception: {e}')
                traceback.print_exc(file=sys.stdout)
                _kw['msg'] = e
            finally:
                _kw['end_ts'] = int(time.time())
                if log_id:
                    resolve_jydata_script_run_log(log_id=log_id, **_kw)
                else:
                    save_jydata_script_run_log(**_kw)
            return result

        return inner
    return wrapper


def func_stream(run_funcs, app=None):
    """
    按顺序依次执行方法列表，如果方法返回值为成功则继续执行下一个方法，否则返回当前方法的结果且不再执行剩下的任务
    :param run_funcs: [(func,  # must
                        params: dict,  # not must, param map
                        data_param_name  # not must, the param key which need the last fun'data returned
                    ),]
    :param app: 调用该方法的app
    :return:
    """
    result = FuncResult(FuncRetCode.NO_HANDLE)
    data = None
    try:
        for run_func in run_funcs:
            func = run_func[0]
            params = None if len(run_func) < 2 else run_func[1]
            if len(run_func) > 2:
                params[run_func[2]] = data
            if params:
                if type(params) == dict:
                    result = func(**params)
                elif type(params) in [list, tuple]:
                    result = func(*params)
                else:
                    result = func(params)
            else:
                result = func()
            if not result:
                if app:
                    app.logger.warning('WARN: the function %s with params {%s} no data returned ' % (
                        func.__name__, params))
                continue
            if result.is_success:
                data = result.data if result.data else data
            elif result.no_handle:
                data = result.data if result.data else []
                if app:
                    app.logger.warning('WARN: the function %s with params {%s} no handle ' % (func.__name__, params))
            elif result.no_data:
                data = []
                if app:
                    app.logger.warning('WARN: the function %s with params {%s} no data returned ' % (
                        func.__name__, params))
            else:
                break
    except Exception as e:
        if app:
            app.logger.error('there has occured errors from %s which caused by %s' % (app.name, e))
        traceback.print_exc(file=sys.stdout)
    return result


def func_parallel(run_funcs):
    """
    并发执行方法列表中的所有方法
    :param run_funcs:
    :return:
    """
    pass


def pre_dependency(func, *args, order_type=OrderType.ORDER):
    """
    run some functions the func depended before run the func
    :param func:
    :param args:
    :param order_type:
    :return:
    """
    run_funcs = []
    for arg in args:
        key, params, data_param_name = arg
        func = FUNC_MAP.get(key)
        run_funcs.append((key, params, data_param_name))
    func_run_switch[order_type](run_funcs)
    return func
