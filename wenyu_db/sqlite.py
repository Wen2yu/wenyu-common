# -*- coding:utf-8 -*-

# Name: py_mysql
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/11 13:15

import sqlite3 as db

from jy_conf import sqlite_infos
from .base_db import BaseDb, BaseDbFactory, parse_db_url


def gen_url(url, **kw):
    url = url or kw.pop('url')
    return f"{url}?{'&'.join([f'{k}={str(v)}' for k, v in kw.items()])}" if kw else url


class Sqlite(BaseDb):
    def new_conn(self, url=None, **kw):
        """
        生成 mysql 链接。 port为空则默认 3306
        :param url: 尽量不用。eg: test:123456@127.0.0.1:3306/mysql?charset=utf8&autocommit=true
        :param kw: dict. eg: {'host': '127.0.0.1', 'user': 'test', 'passwd': '123456', 'db': 'mysql'}
        :return:
        """
        return db.connect(gen_url(url, **kw))

    def gen_query_tables_sql(self, *params, **kw):
        pattern = params[0] if params else kw.get('table_name_pattern', '')
        return f"""select name from sqlite_master where type='table' {f"and name like '{pattern}'" if pattern else ''} order by name"""

    def get_table_columns(self, table_name, cursor=None, show_comments=False):
        cursor.execute(f'pragma table_infO(yj_contract)')
        return cursor.fetchall()

    def save(self, table_name, data, columns=None, **kw):
        sql = f"""
          insert into {table_name}{
            '' if not columns else ('(' + (', '.join(columns) if type(columns) == list else columns) + ')')
          } values (
            {','.join(['%s'] * len(
              (columns if type(columns) == list else columns.split(',')) if columns else self.columns(table_name)
            ))}
          )"""
        return self.update_batch(sql, data, batch_num=kw.pop('batch_num', 100000))


class SqliteFactory(BaseDbFactory):
    instance_cls = Sqlite
    db_type='Sqlite'

    def get_db_conf(self, key: str) -> dict:
        return sqlite_infos.get(key, dict())

    def db_confs(self) -> dict:
        return sqlite_infos
