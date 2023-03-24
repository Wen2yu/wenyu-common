# -*- coding:utf-8 -*-

# Name: base_db
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/11 12:42

import gc
import re
import sys
import time
import traceback
from decimal import Decimal
from functools import wraps
from rangedict import RangeDict

from jy_crypt.jyvip_convert import convert
from jy_entity.base_share_obj import BaseShareObj, BaseShareObjFactory
from jy_utils import str2dict, left_union
from jy_utils.date_utils import *
from jy_utils.list_utils import first
from jy_utils.sys_utils import str_val

max_int = 2 ** 31
_int_range_type_2 = RangeDict()
_int_range_type_2[(0, 99)] = 'TINY'
_int_range_type_2[(100, 9999)] = 'SMALL'
_int_range_type_2[(10000, max_int)] = 'INT'
_int_range_type_2[(-max_int, -1)] = 'INT'

python_type_2 = {
    int: lambda x: _int_range_type_2._find_key(k).value if _int_range_type_2._find_key(k) else 'LONG',
    float: lambda x: 'DOUBLE',
    Decimal: lambda x: 'DECIMAL',
    str: lambda x: 'VAR_STRING',
    date: lambda x: 'DATE',
    datetime: lambda x: 'DATETIME',
    dict: lambda x: 'JSON',
    list: lambda x: 'STRING',
}


def parse_db_url(url: str) -> dict:
    """
    解析mysql 链接
    :param url: eg:'test:123456@127.0.0.1:3306/mysql?charset=utf8&autocommit=true'
    :return:
    """
    result = dict()
    idx1 = url.find('@')
    user_info = url[:idx1] if idx1 > 1 else ''
    if user_info:
        result.update(str2dict(user_info, [':', ], keys=['user', 'passwd']))
    idx2 = url.find('/', idx1 + 1)
    host_info = url[idx1+1:idx2] if idx2 > idx1 + 1 else url[idx1+1:]
    if host_info:
        result.update(str2dict(host_info, [':', ], keys=['host', 'port']))
    if idx2 > 0:
        idx3 = url.find('?', idx2 + 1)
        result['database'] = url[idx2 + 1:idx3] if idx3 > idx2 + 1 else url[idx2+1:]
        if not result['database']:
            result.pop('database')
        if idx3 > 0:
            result.update(str2dict(url[idx3+1:], ['&', '=']))
    return result


chars_dict = {
    '_w': 'abcdefghijklmnopqrstuvwxyz0123456789',
    '_W': 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
    '_Ww': 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789',
    '_d': '0123456789',
}
_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday ']
_month = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']


def random_str(chars, num, repeat=1, **kw):
    return ''.join([chars_dict[chars][random.randint(0, len(chars_dict[chars]) - 1)] for i in range(num)])


def week_str(length=0, n=-1, **kw):
    if n < 0:
        n = now().weekday()
    return _week[n][:length] if length > 0 else _week[n]


def month_str(length=0, n=-1, **kw):
    if n < 0:
        n = now().month - 1
    return _week[n][:length] if length > 0 else _week[n]


func_map = {
    'now': now,
    'day_begin': day_begin,
    'day_end': day_end,
    'yesterday_begin': yesterday_begin,
    'yesterday_end': yesterday_end,
    'month_begin': month_begin,
    'month_end': month_end,
    'last_month_begin': last_month_begin,
    'last_month_end': last_month_end,
    'year_begin': year_begin,
    'year_end': year_end,
    'the_n_workday': the_n_workday,
    'str': str,
    'random_str': random_str,
    'week_str': week_str,
    'month_str': month_str,
}

param_type_switch = {
    'datestr': lambda func, **kw: day_str(func(**kw), fmt=kw.pop('fmt', '%Y-%m-%d %H:%M:%S')),
    'datets': lambda func, **kw: day_ts(func(**kw)),
    'date': lambda func, **kw: func(**kw),
    'quarter': lambda func, **kw: quarter(func(**kw)),
    '': lambda func, **kw: func(**kw),
    'sql': lambda func, **kw: func(kw['db'].select(kw['sql'])[0][0]),
}



def def_param_method(func, **kw):
    return func(**kw) if callable(func) else (
        f"{func.__name__ if func else ''}?{'&'.join([f'{k}={v}' for k, v in kw.items()])}"
    )


def handle_param(param: str, key_pre='#', **kw):
    ss = param.split('?') if param else ['']
    param_type, kws = '', dict()
    if len(ss) > 1:
        ptn = re.compile(f'\{key_pre}([a-zA-Z0-9_]+)')
        names = set(re.findall(ptn, ss[1]))
        kws = {
            k: kw.pop(v.replace(key_pre, '')) if type(v) == str and v.replace(key_pre, '') in names and kw.get(
                v.replace(key_pre, '')
            ) else v for (k, v) in str2dict(ss[1], ['&', '=']).items()
        }
    left_union(kws, kw)
    tf = ss[0].split('#')
    if len(tf) > 1:
        param_type = tf[0]
    func = func_map.get(tf[-1], tf[-1])
    return param_type_switch.get(param_type, def_param_method)(func, **kws) if callable(func) else param


def gen_sql_with_params(sql: str, params: tuple = None, key_pre='#', not_str=False, **kw):
    """
    填充sql中的参数
    :param sql: 原始sql
    :param params: 参数列表
    :param key_pre: 参数dict 参数名前缀标识
    :param kw: 参数dict
    :return:
    """
    result = []
    if params:
        if sql.find('%') > 0:
            sql = (sql.replace('\n', ' ') + ' ').replace('%', '???').replace(' ???s ', ' %s ')\
                .replace('"???s"', '"%s"').replace("'???s'", "'%s'").replace("_???s ", "_%s ")\
                .replace('*???s', '*%s').replace('???s*', '%s*')\
                .replace("('???s'", "('%s'").replace("'???s')", "'%s')")\
                .replace('(???s', '(%s').replace('???s)', '%s)')
        need_params_num = sql.count('%s')
        if len(params) >= need_params_num:
            v = [handle_param(str(param), key_pre=key_pre, **kw) if param is not None else '' for param in params[-need_params_num:]]
            if not_str:
                result = str_val(v)
            sql = sql % tuple(v)
            if sql.find('???') > 0:
                sql = sql.replace('???', '%')
        else:
            print(f'not enough param for sql[{sql}]，{need_params_num} params needed, {len(params)} has given')
            return ''
    if kw:
        ptn = re.compile(f'\{key_pre}([a-zA-Z0-9_]+)')  # 此处报错原因为预编译，正常解决办法有1：使用r'\'; 2: '\\\'。这两个解决办法在这里都不适用，此处报错可以忽略。
        param_names = set(re.findall(ptn, sql))
        for name in sorted(param_names, reverse=True):
            if kw.get(name) is not None:
                v = handle_param(str(kw[name]), key_pre=key_pre, **kw)
                sql = sql.replace(f'{key_pre}{name}', str(v))
                if not_str:
                    result.append(str_val(v))
            else:
                print(f'param:{key_pre + name}{name} in sql[{sql}] has not given')
                return ''
    if not_str and not result:
        return str_val(sql)
    if kw.pop('not_print_sql', True):
        print(f'{sql};')
    return (result[0] if len(result) == 1 else result) if not_str else sql.rstrip()


def parse_data(data, columns):
    return [dict(zip([c[0] for c in columns], d)) for d in data]


def execute(def_result=None, update=0):
    def wrapper(func):

        @wraps(func)
        def inner(self, *args, **kw):
            cursor, result = self.cursor(), def_result
            try:
                kw['cursor'] = cursor
                return func(self, *args, **kw)
            except Exception as e:
                traceback.print_exc()
                print(e)
                if update:
                    self.rollback()
                return result, str(e)
            finally:
                if cursor:
                    cursor.close()
                if self.auto_close:
                    self.close()
                    if self.auto_release:
                        self.freeing()

        return inner

    return wrapper


class BaseDb(BaseShareObj):
    """
    结构型数据库基本操作：orcl/mysql/sqlite
    """
    conn = None
    auto_close = True
    auto_release = True
    debug = True

    def __init__(self, url=None, **kw):
        self.auto_close = kw.pop('auto_close', True)
        self.auto_release = kw.pop('auto_release', True)
        self.debug = kw.pop('debug', True)
        super().__init__(**kw)
        self.db_type = kw.get('db_type')
        self.key = kw.get('db_key')
        self.url = url
        self.conf = {k: convert(v, 1) if k == 'passwd' else v for k, v in kw.items() if k not in ['db_type', 'db_key']}
        # self.check_conn()

    def check_conn(self):
        """
        设置数据库链接
        :return: no return
        """
        try:
            if not self.conn:
                self.conn = self.new_conn(url=self.url, **self.conf)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        return self.conn

    def new_conn(self, url=None, **kw):
        """
        生成数据库连接
        :param url: str. cx_oracle参数，其他数据库不建议用。eg:
            orcl: f'{username}/{passwd}@{host}:{port}/{sid}'. eg: 'test/123456@127.0.0.1:1521/orcl'
            mysql: eg: test:123456@127.0.0.1:3306/mysql?charset=utf8&autocommit=true
            hive: eg: test:123456@127.0.0.1:10000/hive
            impala: eg: test:123456@127.0.0.1:10000/hive
        :param kw: dict. eg: {'host': '127.0.0.1', 'user': 'test', 'passwd': '123456', 'db': 'mysql'}
        :return:
        """
        return None

    def cursor(self):
        self.check_conn()
        return self.conn.cursor()

    def other_db(self, **kw):
        return None

    def has_lob_col(self, col_desc):
        return None

    def convert_desc2common(self, columns_desc, **kw):
        return columns_desc

    @execute(def_result=[])
    def select(self, sql, cursor=None, size=-1, params: tuple = None, key_pre='#', **kw):
        """
        查询数据
        :type size: int 抓取结果条数
        :param sql: select sql
        :param cursor:
        :param params: params
        :param key_pre: 命名参数标识符
        :param kw: 命名参数
        :param include_col_names: 是否返回查询列名，默认不返回
        :return:
        """
        sql = gen_sql_with_params(sql, params=params, key_pre=key_pre, not_print_sql=self.debug, **kw)
        if sql:
            if self.db_type == 'Orcl' and self.conf.get('user'):
                sql = sql.replace(
                    ' from ', f" from {self.conf['user']}."
                ).replace(
                    ' join ', f" join {self.conf['user']}."
                ).replace(
                    f" {self.conf['user']}.(", ' ('
                )
            cursor.execute(sql)
            has_lob_col = self.has_lob_col(cursor.description)
            if has_lob_col and first(has_lob_col, lambda i: i):
                result, idx = [], 0
                for fetch in cursor:
                    result.append(tuple([fetch[i].read() if has_lob_col[i] else fetch[i] for i in range(len(fetch))]))
                    if 0 < idx < size:
                        idx += 1
                    else:
                        break
            else:
                result = cursor.fetchall() if size < 0 else (
                    cursor.fetchone() if size == 1 else cursor.fetchmany(size=size)
                )
        else:
            result = []
        if not kw.get('include_col_names', 0):
            return result
        else:
            return result, self.convert_desc2common(cursor.description, **kw)

    @execute(def_result=0, update=1)
    def update(self, sql, params=None, cursor=None, **kw):
        """
        insert/update/delete data
        :param sql: insert/update/delete sql
        :param params: params of update sql
        :param cursor:
        :return:
        """
        if sql.find('%s') > 0 or sql.find(kw.get('key_pre', '#')) > 0:
            sql = gen_sql_with_params(sql, params=params, key_pre=kw.pop('key_pre', '#'), not_print_sql=self.debug, **kw)
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        self.commit()
        return 1

    @execute(def_result=0, update=1)
    def update_batch(self, sql, params=None, batch_num=10000, cursor=None, **kw):
        """
        batch insert/update/delete data
        :param sql: insert/update/delete sql
        :param params: batch params(list)
        :param batch_num: batch num, the num of per commit, default 10000
        :param cursor:
        :return:
        """
        while params:
            try:
                # print(params[0:batch_num])
                cursor.executemany(gen_sql_with_params(sql, not_print_sql=self.debug, **kw), params[0:batch_num])
            except Exception as e:
                l = min(len(params), 20)
                print(params[:l] if l <= 20 else (params[:l/2] + params[-l/2:]))
                traceback.print_exc(file=sys.stdout)
                return 0, e
            params = params[batch_num:]
            print(gc.collect())
        self.commit()
        return 1

    @execute(def_result=[], update=1)
    def exec_sqls(self, sqls, cursor=None):
        o_auto_close = self.auto_close
        self.auto_close = False
        update, result = False, []
        for sql in sqls:
            if sql.get('db'):
                db = self.other_db(**sql['db'])
                if db and sql.get('sqls'):
                    db.exec_sqls(sql['sqls'])
                continue
            data = []
            if sql['sql_type'] == 'insert':
                update = True
                params = sql['params'] if sql.get('params', None) else (
                    eval(sql['fun'])(data) if sql.get('fun', None) else data
                )
                if type(data) == list:
                    data = cursor.executemany(sql['sql'], params=params)
                else:
                    data = cursor.execute(sql['sql'], params=params)
            else:
                if sql['sql_type'] == 'proc':
                    args = [
                        gen_sql_with_params(i['val'], not_str=True, **i.get('kw', dict())) if type(i) == dict else (
                            gen_sql_with_params('#arg', arg=i, not_str=True) if type(i) == str else i
                        ) for i in sql.get('args', [])
                    ]
                    print(args)
                    data = cursor.callproc(args[0], args[1:])
                    print(args, data)
                else:
                    exec_sql = gen_sql_with_params(sql['sql'], *tuple(sql.get('args', [])), not_print_sql=self.debug, db=self, **sql.get('kw', dict()))
                    cursor.execute(exec_sql)
                    if sql['sql_type'] == 'select':
                        size = sql.get('kw', dict()).get('size', -1)
                        data = cursor.fetchall() if size < 0 else (
                            cursor.fetchone() if size == 1 else cursor.fetchmany(size=size)
                        )
                    else:
                        data = 1
                        update = True
                if sql['sql_type'] == 'create_table':
                    time.sleep(5)
            result.append(data)
        if update:
            self.commit()
        self.auto_close = o_auto_close
        return result

    @execute(def_result=0)
    def exec_proc(self, *args, cursor=None):
        args = [
            gen_sql_with_params(i['val'], not_str=True, **i.get('kw', dict())) if type(i) == dict else (
                gen_sql_with_params('#arg', arg=i, not_str=True) if type(i) == str else i
            ) for i in args
        ]
        print(args)
        return cursor.callproc(args[0], args[1:])

    @execute(def_result=0)
    def create_table(self, sql='', params: tuple = None, cursor=None, **kw):
        sql = gen_sql_with_params(sql, params=params, not_print_sql=self.debug, **kw) if sql else self.gen_create_table_sql(**kw)
        if sql:
            print(sql)
            cursor.execute(sql)
            table_comments_sqls = self.gen_create_table_comments_sqls(
                table_name=kw.get('table_name'), table_comment=kw.get('table_comment'), col_comments=kw.get('col_comments')
            )
            print(table_comments_sqls)
            for s in table_comments_sqls:
                cursor.execute(s)
        return sql

    @execute(def_result=[])
    def tables(self, *params, cursor=None, **kw):
        sql = self.gen_query_tables_sql(*params, **kw)
        cursor.execute(sql)
        return cursor.fetchall()

    def check_table_name(self, table_name):
        _s = table_name.find(' ')
        return table_name[:_s] if _s > 0 else table_name

    def gen_create_table_sql(self, table_name, columns_desc, **kw):
        pass

    def gen_create_table_comments_sqls(self, table_name, table_comment=None, col_comments: dict = None, **kw):
        return []

    @execute(def_result=[])
    def columns(self, table_name, cursor=None, show_comments=False, **kw):
        return self.get_table_columns(table_name, cursor=cursor, show_comments=show_comments)

    @execute(def_result='')
    def table_comment(self, table_name, cursor=None):
        return self.get_table_comment(table_name, cursor=cursor)

    def gen_query_tables_sql(self, *params, **kw):
        pass

    def get_table_comment(self, table_name, cursor=None):
        pass

    def get_table_columns(self, table_name, cursor=None, show_comments=False):
        pass

    def save(self, table_name, data, database=None, columns=None, **kw):
        pass

    def delete(self, table_name, database=None, **kw):
        pass

    def commit(self):
        try:
            self.conn.commit()
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    def rollback(self):
        try:
            self.conn.rollback()
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None



class BaseDbFactory(BaseShareObjFactory):
    # 数据库集
    instance_dict = dict()
    # 数据库对象类型
    instance_cls = BaseDb

    def get_db(self, key: str = None, **kw):
        """
        获取 数据库
        :param key: 数据库标识
        :param kw: 数据库链接创建参数
        :return:
        """
        result = None
        try:
            kw = self.gen_attrs(key, db_type=self.db_type, db_key=key, **kw)
            result = self.get_instance(key, **kw)
        except Exception as e:
            traceback.print_exc()
        return result

    def get_db_conf(self, key: str) -> dict:
        """
        获取数据库实例配置信息
        :param key: 数据库实例标识
        :return:
        """
        return dict()

    def db_confs(self) -> dict:
        """
        获取全部数据库实例配置信息
        :return:
        """
        return dict()

    def gen_attrs(self, key, **kw):
        return left_union(kw, self.get_db_conf(key))
