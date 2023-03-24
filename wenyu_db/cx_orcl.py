# -*- coding:utf-8 -*-

# Name: cx_orcl
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/11 12:55

import cx_Oracle as db
import psycopg2 as pg

from jy_conf import orcl_infos
from jy_utils.list_utils import first, to_list
from jy_utils.obj_utils import none

from .base_db import BaseDb, BaseDbFactory, execute, gen_sql_with_params


col_type_2 = {
    'DECIMAL': lambda *args: args[1] == db.NUMBER and args[4] and args[5] >= 2,
    'TINY': lambda *args: args[1] == db.NUMBER and 0 < args[4] <= 2 and args[5] == 0,
    'SHORT': lambda *args: args[1] == db.NUMBER and 2 < args[4] <= 4 and args[5] == 0,
    'LONG': lambda *args: args[1] == db.NUMBER and 4 < args[4] <= 11 and args[5] == 0,
    'FLOAT': lambda *args: args[1] == db.NUMBER and args[4] and args[5] == 1,
    'DOUBLE': lambda *args: args[1] == db.NUMBER and args[4] == 0,
    'NULL': lambda *args: args[1] == db.STRING and args[2] == 0,
    'TIMESTAMP': lambda *args: args[1] == db.TIMESTAMP,
    'LONGLONG': lambda *args: args[1] == db.LONG_STRING or (args[1] == db.NUMBER and args[4] > 24),
    'INT24': lambda *args: args[1] == db.NUMBER and 11 < args[4] <= 24,
    'DATE': lambda *args: args[1] == db.Date,
    'DATETIME': lambda *args: args[1] == db.DATETIME,
    'VARCHAR': lambda *args: args[1] == db.FIXED_CHAR,
    'BLOB': lambda *args: args[1] == db.BLOB,
    'VAR_STRING': lambda *args: args[1] == db.STRING and args[2],
    'STRING': lambda *args: args[1] == db.CLOB,
}


polardb_type_2 = {
    0: 'VARCHAR',
    23: 'INT',
    1043: 'VARCHAR',
    1700: 'DECIMAL',
    1114: 'DATETIME',
}


def _number_2(precision, scale, *args):
    result = ['INT', 18, 0]
    if not scale:
        if precision <= 2:
            result = ['TINY', precision, scale]
        elif precision <= 4:
            result = ['SHORT', precision, scale]
        elif precision <= 11:
            result = ['INT', precision, scale]
        elif precision <= 24:
            result = ['INT24', precision, scale]
        else:
            result = ['LONG', precision, scale]
    elif scale <= 6:
        result = ['DECIMAL', precision, scale]
    elif scale < 1000:
        result = ['DOUBLE', precision, scale]
    return result


polardb_typename_2 = {
    'VARCHAR': lambda *args: ['VARCHAR'] + args,
    'VARCHAR2': lambda *args: ['VARCHAR'] + args,
    'DATE': lambda *args: ['DATETIME'] + args,
    'NUMBER': lambda *args: _number_2(*args)
}


col_len_2 = {
    db.NUMBER: lambda *args: [args[4], args[5]],
    db.STRING: lambda *args: [args[2], args[3]],
    db.FIXED_CHAR: lambda *args: [args[2], args[3]],
    db.LONG_STRING: lambda *args: [args[2], args[3]],
    db.DATETIME: lambda *args: [args[2], args[3]],
    db.TIMESTAMP: lambda *args: [args[2], args[3]],
    db.CLOB: lambda *args: [args[2], args[3]],
    db.BLOB: lambda *args: [args[2], args[3]],
}


_2_orcl_type = {
    'DECIMAL': lambda *args: f'NUMBER({args[0] if args else 10},{args[1] if len(args) > 1 else 2})',
    'TINY': lambda *args: 'NUMBER(2, 0)',
    'SHORT': lambda *args: 'NUMBER(4, 0)',
    'LONG': lambda *args: 'NUMBER(11, 0)',
    'FLOAT': lambda *args: 'NUMBER(11, 1)',
    'DOUBLE': lambda *args: 'NUMBER',
    'NULL': lambda *args: 'VARCHAR2(48)',
    'TIMESTAMP': lambda *args: 'TIMESTAMP',
    'LONGLONG': lambda *args: 'NUMBER(11, 0)',
    'INT24': lambda *args: 'NUMBER(24)',
    'DATE': lambda *args: 'DATE',
    'TIME': lambda *args: 'DATE',
    'DATETIME': lambda *args: 'DATE',
    'YEAR': lambda *args: 'DATE',
    'NEWDATE': lambda *args: 'DATE',
    'VARCHAR': lambda *args: f'VARCHAR2({args[0] if args else 48})',
    'BIT': lambda *args: 'NUMBER(3, 0)',
    'JSON': lambda *args: 'CLOB',
    'NEWDECIMAL': lambda *args: f'DECIMAL({args[0] if args else 10},{args[1] if len(args) > 1 else 2})',
    'ENUM': lambda *args: 'NUMBER(2)',
    'SET': lambda *args: 'NUMBER(18, 0)',
    'TINY_BLOB': lambda *args: 'VARCHAR2(256)',
    'MEDIUM_BLOB': lambda *args: 'BLOB',
    'LONG_BLOB': lambda *args: 'BLOB',
    'BLOB': lambda *args: 'BLOB',
    'VAR_STRING': lambda *args: f'VARCHAR2({args[0] if args else 1024})',
    'STRING': lambda *args: 'CLOB',
    'GEOMETRY': lambda *args: 'VARCHAR2(48)'
}


class Orcl(BaseDb):
    def new_conn(self, url: str = '', **kw):
        """
        生成 oracle 数据库连接， port为空则默认1521
        :param url: f'{username}/{passwd}@{host}:{port}/{sid}'. eg: 'test/123456@127.0.0.1:1521/orcl'
        :param kw: dict. eg:{'username': 'test', 'passwd': '123456', 'host': '127.0.0.1', 'port': 1521, 'sid': 'orcl'}
        :return:
        """
        if kw.pop('polardb', 0):
            return pg.connect(user=kw.pop('username', None), **kw)
        else:
            if not url:
                url = f'{kw.get("username", "")}/{kw.get("passwd", "")}@{kw.get("host", "")}:{kw.get("port", 1521)}/{kw.get("sid", "orcl")}'
            return db.connect(url)

    def has_lob_col(self, col_desc):
        return [i[1] in [db.BLOB, db.CLOB, db.CLOB] for i in col_desc]

    def convert_desc2common(self, columns_desc, **kw):
        result = []
        if self.conf.get('polardb', 0):
            for i in columns_desc:
                col_name = getattr(i, 'name').lower()
                if kw.get('tablename'):
                    columns = self.columns(kw['tablename'])
                    column = first(columns, lambda c: c[0].lower() == col_name, (col_name, 'VARCHAR2', 0, 1024, 0))
                    result.append(tuple([col_name,] + polardb_typename_2[column[1]](int(none(column[3],0)), int(none(column[4], 0)))))
                else:
                    result.append((
                        col_name, polardb_type_2[getattr(i, 'type_code', 0)], getattr(i, 'precision', 0), getattr(i, 'scale', 0)
                    ))
        else:
            result = [tuple(
                [i[0].lower(), next(filter(lambda di: col_type_2[di](*i), col_type_2))] + col_len_2.get(i[1], lambda *a: [0, 0])(*i)
            ) for i in columns_desc]
        return result

    def save(self, table_name, data, columns=None, **kw):
        syb = ','.join([
            '%s' if self.conf.get('polardb') else (':%d' % (i + 1)) for i in range(len(
                (columns if type(columns) == list else columns.split(',')) or self.columns(table_name)
            ))
        ])
        cols_str = ', '.join(columns) if type(columns) == list else columns
        sql = f"insert into {table_name}{'' if not columns else f'({cols_str})'} values ({syb})"
        return self.update_batch(sql, data, batch_num=kw.pop('batch_num', 100000))

    def delete(self, table_name, **kw):
        _where = ' and '.join([f"""{k} = {f"'#{k}'" if type(kw[k]) == str else f'#{k}'}""" for k in kw])
        self.update(f"delete from {table_name} where {'1 = 1' if _where else _where}", **kw)

    @execute(def_result=0)
    def exec_func(self, *args, cursor=None):
        return cursor.callfunc(*args)

    def gen_query_tables_sql(self, *params, **kw):
        return gen_sql_with_params(
            f"""select table_name from {
              f"all_tables where OWNER = '{self.conf['user']}' and" if self.conf.get('user') else 'user_tables where'
            } table_name #condition_symbol upper('%s')""",
            to_list(params)[0] if params else (kw.get('table_name_pattern', '%'),),
            condition_symbol=kw.get('condition_symbol', 'like')
        )

    def get_table_comment(self, table_name, cursor=None):
        if self.conf.get('polardb'):
            return ''
        cursor.execute(f"""select comments from {
            f"all_tab_comments where OWNER = '{self.conf['user']}' and" if self.conf.get('user') else 'USER_tab_COMMENTS where'
        } table_name = upper('{table_name}')""")
        return cursor.fetchone()[0]

    def get_table_columns(self, table_name: str, cursor=None, show_comments=False):
        cursor.execute(f"""select COLUMN_NAME, DATA_TYPE, {'0 as ' if self.conf.get('polardb') else ''}DATA_TYPE_MOD, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT from {
            f"all_tab_cols where OWNER = '{self.conf['user']}' and" if self.conf.get('user') else 'user_tab_cols where'
        } table_name = '{table_name.upper()}'""")
        columns = cursor.fetchall()
        if show_comments and not self.conf.get('polardb'):
            cursor.execute(f"""select COLUMN_NAME, COMMENTS from {
                f"all_col_comments where OWNER = '{self.conf['user']}' and" if self.conf.get('user') else 'user_col_comments where'
            } table_name = '{table_name.upper()}'""")
            comments = cursor.fetchall()
            columns = [tuple(col + first(comments, lambda i: i[0] == col[0], def_val=(None, None))[1:2]) for col in
                       columns]
        return columns

    def gen_create_table_sql(self, table_name=None, columns_desc=None, **kw):
        body = ',\n  '.join([
            f'{i[0]} {_2_orcl_type[i[1]](*(i[2], i[3]))}' for i in columns_desc
        ])
        return f"create table {table_name} (\n  {body}\n)"

    def gen_create_table_comments_sqls(self, table_name, table_comment=None, col_comments: dict = None):
        sqls = []
        if table_comment:
            sqls.append(f"comment on table {table_name} is '{table_comment}'")
        if col_comments:
            sqls.extend([
                f"comment on column {table_name}.{col_name} is '{comment}'" for col_name, comment in col_comments.items()
            ])
        return sqls


class OrclFactory(BaseDbFactory):
    instance_cls = Orcl
    db_type = 'Orcl'

    def get_db_conf(self, key: str) -> dict:
        return orcl_infos.get(key, dict())

    def db_confs(self) -> dict:
        return orcl_infos
