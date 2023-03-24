# -*- coding:utf-8 -*-

# Name: impala
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/17 9:33
import time

import impala.dbapi as db

from jy_conf import impala_infos
from .base_db import BaseDb, BaseDbFactory, parse_db_url
from .py_hive import HiveFactory, Hive


class Impala(BaseDb):

    hive: Hive = None

    def __init__(self, url=None, **kw):
        self.hive = HiveFactory().get_db(key=kw.pop('hive_key'))
        super().__init__(url, **kw)

    def new_conn(self, url=None, **kw):
        """
        生成 hive 链接
        :param url: 尽量不用。eg: 127.0.0.1:21050
        :param kw: dict. eg: {'host': '127.0.0.1', 'port': 21050}
        :return:
        """
        if url:
            kw.update(parse_db_url(url))
        return db.connect(**kw)

    def other_db(self, **kw):
        if kw.get('db_type') == 'Hive':
            return HiveFactory().get_db(key=kw.get('key'))
        return None

    def convert_desc2common(self, columns_desc, **kw):
        return self.hive.convert_desc2common(columns_desc, **kw)

    def save(self, table_name, data, database=None, data_name_labels=('0000-0', ), **kw):
        """
        保存数据
        :param table_name:
        :param data:
        :param database:
        :param data_name_labels:
        :param kw:
        :return:
        """
        result = self.hive.save(table_name, data, database=database, data_name_labels=data_name_labels, **kw)
        t_name = f'{database}.{table_name}'
        if t_name:
            self.create_table(f'invalidate metadata {t_name}')
        return result

    def tables(self, *params, cursor=None, **kw):
        return self.hive.tables(*params, **kw)

    def create_table(self, sql='', params: tuple = None, cursor=None, **kw):
        if sql.startswith('invalidate metadata '):
            result = super().create_table(sql=sql, params=params, **kw)
        else:
            result = self.hive.create_table(sql=sql, params=params, **kw)
        time.sleep(5)
        return result

    def columns(self, table_name, cursor=None, show_comments=False):
        return self.hive.columns(table_name, show_comments=show_comments)

    def commit(self):
        pass

    def rollback(self):
        pass

    def _db_type(self, src_type, *args):
        return self.hive._db_type(src_type)


class ImpalaFactory(BaseDbFactory):
    instance_cls = Impala
    db_type='Impala'

    def get_db_conf(self, key: str) -> dict:
        return impala_infos.get(key, dict())

    def db_confs(self) -> dict:
        return impala_infos
