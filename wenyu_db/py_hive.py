# -*- coding:utf-8 -*-

# Name: py_hive
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/16 19:40

import os
from pyhive import hive as db
from thrift.transport.TTransport import TTransportException

from jy_conf import hive_infos, hdfs_infos
from jy_utils.date_utils import day_str, datetime, from_timestamp, ZERO_DATETIME
from jy_utils.num_utils import format_number
from jy_utils.sys_utils import str_val
from .base_db import BaseDb, BaseDbFactory, parse_db_url, handle_param
from .hdfs import Hdfs


partition_key_func_switch = {
    'left': lambda i, *args: (day_str(i) if type(i) == datetime else (
        day_str(from_timestamp(i)) if type(i) in [int, float] else i
    ))[:args[0]],
    'right': lambda i, *args: (day_str(i) if type(i) == datetime else (
        day_str(from_timestamp(i)) if type(i) in [int, float] else i
    ))[-args[0]:],
    'substr': lambda i, *args: (day_str(i) if type(i) == datetime else (
        day_str(from_timestamp(i)) if type(i) in [int, float] else i
    ))[args[0]:args[0]+args[1]],
    'format_number': lambda i, *args: format_number(i, *args),
    'mod': lambda i, *args: i % args[0],
    'trunc': lambda i, *args: int(i/(args[0] if args else 1)),
    'val': lambda i, *args: ','.join(args)
}

col_type_2 = {
    'TINYINT_TYPE': 'TINY',
    'SMALLINT': 'SHORT',
    'INT_TYPE': 'LONG',
    'BIGINT_TYPE': 'LONGLONG',
    'FLOAT_TYPE': 'FLOAT',
    'DOUBLE_TYPE': 'DOUBLE',
    'DECIMAL_TYPE': 'DECIMAL',
    'STRING_TYPE': 'VAR_STRING',
    'VARCHAR_TYPE': 'STRING',
    'CHAR_TYPE': 'VARCHAR',
    'TIMESTAMP_TYPE': 'TIMESTAMP',
    'DATE_TYPE': 'DATE',
    'STRING': 'VAR_STRING',
    'TINYINT': 'TINY',
    'BIGINT': 'INT',
    'INT': 'INT',
    'DOUBLE': 'DOUBLE',
}


_2_hive_type = {
    'DECIMAL': 'double',
    'TINY': 'tinyint',
    'SHORT': 'smallint',
    'LONG': 'int',
    'FLOAT': 'float',
    'DOUBLE': 'double',
    'NULL': 'string',
    'TIMESTAMP': 'timestamp',
    'LONGLONG': 'bigint',
    'INT24': 'bigint',
    'DATE': 'date',
    'TIME': 'string',
    'DATETIME': 'string',
    'YEAR': 'date',
    'NEWDATE': 'date',
    'VARCHAR': 'string',
    'BIT': 'tinyint',
    'JSON': 'string',
    'NEWDECIMAL': 'double',
    'ENUM': 'tinyint',
    'SET': 'int',
    'TINY_BLOB': 'string',
    'MEDIUM_BLOB': 'string',
    'LONG_BLOB': 'string',
    'BLOB': 'string',
    'VAR_STRING': 'string',
    'STRING': 'string',
    'GEOMETRY': 'string',
}


class Hive(BaseDb):

    def_database = None
    hdfs: Hdfs = None
    hdfs_conf = dict()

    def __init__(self, url=None, **kw):
        self.hdfs_path = kw.pop('hdfs_path')
        self.hdfs = self._hdfs(kw.pop('hdfs_key', None), **kw.pop('hdfs', dict()))
        super().__init__(url, **kw)

    def _hdfs(self, key=None, **kw):
        """
        获取hdfs客户端
        :param key:
        :param kw:
        :return:
        """
        if key:
            self.hdfs_conf = hdfs_infos.get(key, dict())
        self.hdfs_conf.update(kw.pop('hdfs', dict()))
        return Hdfs(key, **self.hdfs_conf)

    def new_conn(self, url=None, **kw):
        """
        生成 hive 链接
        :param url: 尽量不用。eg: test:123456@127.0.0.1:10000/hive
        :param kw: dict. eg: {'host': '127.0.0.1', 'user': 'test', 'passwd': '123456', 'database': 'hive'}
        :return:
        """
        self.def_database = kw.get('database')
        if url:
            kw.update(parse_db_url(url))
        return db.Connection(**kw)

    def convert_desc2common(self, columns_desc, **kw):
        return [
            (i[0].split('.')[-1], col_type_2[i[1]], 0, 0) for i in columns_desc
        ]

    def gen_query_tables_sql(self, *params, database=None, **kw):
        pattern = params[0] if params else kw.get('table_name_pattern', '')
        print(pattern)
        return f"""show tables{f" from {database}" if database else ''}{" like '%s'" % pattern if pattern else ''}"""

    def get_table_columns(self, table_name, cursor=None, show_comments=False, **kw):
        if kw.get('database'):
            table_name = f"{kw['database']}.{table_name}"
        cursor.execute(f'desc {self.check_table_name(table_name)}')
        columns = []
        for col in cursor.fetchall():
            if col[0]:
                if col[0] != '# Partition Information':
                    columns.append(col[:-1] if show_comments else col)
                else:
                    columns = columns[:-1]
                    break
        return columns

    def gen_create_table_sql(self, table_name=None, columns_desc=None, database='', **kw):
        col_comments = kw.get('col_comments', dict())
        body = ',\n              '.join([
            f"""`{i[0]}` {_2_hive_type[i[1]]}{f" COMMENT '{col_comments[i[0]]}'" if col_comments.get(i[0]) else ''}""" for i in columns_desc
        ])
        split = kw.get('col_split', '\\t')
        return f"""
            CREATE EXTERNAL TABLE `{f'{database}.'if database else ''}{table_name}` (
              {body}
            ){f" COMMENT '{kw['table_comment']}'" if kw.get('table_comment') else ''}{f" partitioned by ({kw['partition'].split(',')[0]} string)" if kw.get('partition') else ''}
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
            WITH SERDEPROPERTIES('field.delim'='{split}', 'serialization.format'='{split}') 
            STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        """

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
        if kw.get('hdfs_key') or kw.get('hdfs'):
            self.hdfs = self._hdfs(kw.pop('hdfs_key', None), **kw.pop('hdfs', dict()))
        data_name_labels = [handle_param(format_number(dnl, min_len=kw.pop('min_num_len', 11))) if dnl else '' for dnl in data_name_labels]
        local_path = f"/home/oracle/{database or self.def_database}_{table_name}_{'_'.join(data_name_labels)}"
        aim_path = f"{self.hdfs_path}{database or self.def_database}.db/{table_name}/"
        col_split = kw.pop('col_split', '\t')
        columns = kw.pop('columns', '')
        partition = kw.pop('partition').split(',') if kw.get('partition') else None
        if partition and len(partition) > 1:
            partition_col_name, idx, partition_key_func = partition[0], int(partition[1]), None
            if len(partition) > 2:
                partition_key_func = partition_key_func_switch.get(partition[2], lambda i, *args: i)
            else:
                partition_key_func = lambda i, *args: i
            _data = dict()
            for d in data:
                args = [str_val(i) for i in partition[3:]]
                print(args)
                partition_key = partition_key_func(
                    d[idx] if d[idx] else(ZERO_DATETIME if partition_col_name.find('date') > 0 else ' ' * args[1]), *args
                ) if partition_key_func else d[idx]
                if _data.get(partition_key):
                    _data[partition_key].append(d)
                else:
                    _data[partition_key] = [d, ]
            data = _data
        else:
            data, partition_col_name = {'data': data}, None
        print(f'>>>__++++{list(data.keys())}, {partition}')
        for k, v in data.items():
            with open(local_path, 'w') as f:
                f.write('\n'.join([col_split.join(
                    [(str(j).replace('\n', '<br>').replace('\r', ' ').replace(col_split, '&nbsp;') if j is not None else '') for j in i]
                ) for i in v]))
            print(partition_col_name, partition)
            hdfs_path = f"{aim_path}/{f'{partition_col_name}={k}/' if partition else ''}data_{'_'.join(data_name_labels)}"
            print(f"hive.save: table_name={table_name}&local_path={local_path}&aim_path={hdfs_path}")
            while 1:
                try:
                    if partition:
                        self.create_table(f"alter table `{database or self.def_database}.{table_name}` add IF NOT EXISTS partition ({partition_col_name}='{k}')")
                    self.hdfs.delete(hdfs_path, recursive=kw.pop('recursive', False), skip_trash=kw.pop('skip_trash', True))
                    self.hdfs.upload(hdfs_path, local_path, **kw)
                    break
                except TTransportException:
                    self.hdfs = self._hdfs(**self.hdfs_conf)
            os.remove(local_path)
            v = None
        return 1

    def commit(self):
        pass

    def rollback(self):
        pass

    def _db_type(self, src_type, *args):
        return _2_hive_type[src_type]


class HiveFactory(BaseDbFactory):
    instance_cls = Hive
    db_type='Hive'

    def get_db_conf(self, key: str) -> dict:
        return hive_infos.get(key, dict())

    def db_confs(self) -> dict:
        return hive_infos
