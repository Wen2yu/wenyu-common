# -*- coding:utf-8 -*-

# Name: py_mysql
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/11 13:15

import pymysql as db

from jy_conf import mysql_infos
from .base_db import BaseDb, BaseDbFactory, parse_db_url


col_type_2 = {
    0: 'DECIMAL',
    1: 'TINY',
    2: 'SHORT',
    3: 'LONG',
    4: 'FLOAT',
    5: 'DOUBLE',
    6: 'NULL',
    7: 'TIMESTAMP',
    8: 'LONGLONG',
    9: 'INT24',
    10: 'DATE',
    11: 'TIME',
    12: 'DATETIME',
    13: 'YEAR',
    14: 'NEWDATE',
    15: 'VARCHAR',
    16: 'BIT',
    245: 'JSON',
    246: 'NEWDECIMAL',
    247: 'ENUM',
    248: 'SET',
    249: 'TINY_BLOB',
    250: 'MEDIUM_BLOB',
    251: 'LONG_BLOB',
    252: 'BLOB',
    253: 'VAR_STRING',
    254: 'STRING',
    255: 'GEOMETRY',
}

_2_mysql_type = {
    'INT': lambda *args: 'INT11',
    'DECIMAL': lambda *args: f'DECIMAL({args[0] if args else 10},{args[1] if len(args) > 1 else 2})',
    'TINY': lambda *args: 'TINYINT',
    'SHORT': lambda *args: 'SMALLINT',
    'LONG': lambda *args: 'INT',
    'FLOAT': lambda *args: 'FLOAT',
    'DOUBLE': lambda *args: 'DOUBLE',
    'NULL': lambda *args: 'VARCHAR(48)',
    'TIMESTAMP': lambda *args: 'TIMESTAMP',
    'LONGLONG': lambda *args: 'BIGINT',
    'INT24': lambda *args: 'BIGINT',
    'DATE': lambda *args: 'DATE',
    'TIME': lambda *args: 'TIME',
    'DATETIME': lambda *args: 'DATETIME',
    'YEAR': lambda *args: 'YEAR',
    'NEWDATE': lambda *args: 'DATE',
    'VARCHAR': lambda *args: f'VARCHAR({args[0] if args else 48})',
    'BIT': lambda *args: 'BIT',
    'JSON': lambda *args: 'JSON',
    'NEWDECIMAL': lambda *args: f'DECIMAL({args[0] if args else 10},{args[1] if len(args) > 1 else 2})',
    'ENUM': lambda *args: 'TINYINT',
    'SET': lambda *args: 'INT',
    'TINY_BLOB': lambda *args: 'TINY_BLOB',
    'MEDIUM_BLOB': lambda *args: 'MEDIUM_BLOB',
    'LONG_BLOB': lambda *args: 'LONG_BLOB',
    'BLOB': lambda *args: 'BLOB',
    'VAR_STRING': lambda *args: f'VARCHAR({args[0] if args else 1024})',
    'STRING': lambda *args: 'TEXT',
    'GEOMETRY': lambda *args: 'GEOMETRY'
}

_num_switch = {
    1: 'TINY',
    2: 'TINY',
    3: 'SHORT',
    4: 'SHORT',
    5: 'LONG',
    6: 'LONG',
    7: 'LONG',
    8: 'LONG',
    9: 'LONG',
    10: 'LONG',
    11: 'LONG',
    12: 'LONG',
}


class Mysql(BaseDb):
    def new_conn(self, url=None, **kw):
        """
        生成 mysql 链接。 port为空则默认 3306
        :param url: 尽量不用。eg: test:123456@127.0.0.1:3306/mysql?charset=utf8&autocommit=true
        :param kw: dict. eg: {'host': '127.0.0.1', 'user': 'test', 'passwd': '123456', 'db': 'mysql'}
        :return:
        """
        if url:
            kw.update(parse_db_url(url))
        return db.connect(**kw)

    def convert_desc2common(self, columns_desc, **kw):
        return [
            (i[0].lower(), _num_switch[i[4]] if i[1] == 8 and 0 < i[4] < 12 else col_type_2[i[1]], i[4], i[5]) for i in columns_desc
        ]

    def save(self, table_name, data, columns=None, **kw):
        sql = f"""
          insert into {self.check_table_name(table_name)}{
            '' if not columns else ('(' + (', '.join(columns) if type(columns) == list else columns) + ')')
          } values (
            {','.join(['%s'] * len(
              (columns if type(columns) == list else columns.split(',')) if columns else self.columns(self.check_table_name(table_name))
            ))}
          )"""
        return self.update_batch(sql, data, batch_num=kw.pop('batch_num', 100000))

    def delete(self, table_name, **kw):
        _where = ' and '.join([f"""{k} = {f"'#{k}'" if type(kw[k]) == str else f'#{k}'}""" for k in kw])
        self.update(f"delete from {self.check_table_name(table_name)} where {'1 = 1' if _where else _where}", **kw)

    def gen_query_tables_sql(self, *params, **kw):
        pattern = params[0] if params else kw.get('table_name_pattern', '')
        print(pattern)
        return f"""show tables{" like '%s'" % pattern if pattern else ''}"""

    def get_table_comment(self, table_name, cursor=None):
        cursor.execute(f"select table_comment from information_schema.tables where table_name = '{self.check_table_name(table_name)}'")
        return cursor.fetchone()[0]

    def get_table_columns(self, table_name, cursor=None, show_comments=False):
        if show_comments:
            cursor.execute(f'show full fields from {self.check_table_name(table_name)}')
        else:
            cursor.execute(f'desc {self.check_table_name(table_name)}')
        return cursor.fetchall()

    def gen_create_table_sql(self, table_name=None, columns_desc=None, **kw):
        col_comments = kw.get('col_comments', dict())
        body = ',\n  '.join([
            f"""`{i[0]}` {_2_mysql_type[i[1]](*(i[2], i[3])) if kw.get('common_type', 1) else i[1]}{f" COMMENT '{col_comments[i[0]]}'" if col_comments.get(i[0]) else ''}""" for i in columns_desc
        ])
        return f"""create table {table_name} (
          {body}
        ){f" COMMENT = '{kw['table_comment']}'" if kw.get('table_comment') else ''}"""

    def _db_type(self, src_type, *args):
        return _2_mysql_type[src_type](*args)


class MysqlFactory(BaseDbFactory):
    instance_cls = Mysql
    db_type='Mysql'

    def get_db_conf(self, key: str) -> dict:
        return mysql_infos.get(key, dict())

    def db_confs(self) -> dict:
        return mysql_infos
