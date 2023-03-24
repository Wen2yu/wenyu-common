# -*- coding:utf-8 -*-

# Name: es
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/11/25 16:39

from elasticsearch import Elasticsearch, helpers
import certifi

from jy_conf.db.es import es_infos
from .base_db import BaseDb, BaseDbFactory


class Es(BaseDb):

    cli = None

    def __init__(self, url=None, **kw):
        super().__init__(url, **kw)
        self.cli = Elasticsearch(
            kw.get('hosts', []),
            http_auth=(kw.get('user', ''),kw.get('passwd', '')),
            use_ssl=kw.get('use_ssl', False)
        )

    def tables(self, *params, **kw):
        index = kw.get('index', params[0])
        table_names = []
        try:
            _index = self.cli.indices.get(index=index)
            if _index:
                table_names = [i for i in _index.get(index, dict()).get('mappings', dict()) if i != '_default_']
        except:
            pass
        return table_names

    def create_index(self, index='', **kw):
        type_name = kw.get('doc_type', index)
        body = {'mappings': {type_name: {'properties': dict()}}}
        # if kw.get('row_key'):
        #     body['mappings'][type_name]["_id"] = {'path': kw['row_key']}
        columns, date_columns = kw.get('columns', []), kw.get('date_columns', [])
        for column in columns:
            body['mappings'][type_name]['properties'][column] = {
                'type': 'date', 'format': 'strict_date_optional_time||epoch_millis||yyyy-MM-dd HH:mm:ss'
            } if date_columns and date_columns.__contains__(column) else {'type': 'keyword'}
        try:
            resp = self.cli.indices.create(index=index, body=body)
            return 1 if resp.get('acknowledged') else 0
        except Exception as e:
            return e

    def create_table(self, table_name='', **kw):
        kw['doc_type'] = table_name
        return self.create_index(index=kw.pop('index', table_name), **kw)

    def add(self, index='', **kw):
        self.cli.index(index=index, doc_type=kw.get('doc_type', index), document=kw.get('body', dict()))

    def bulk(self, body, **kw):
        helpers.bulk(self.cli, body, **kw)

    def save(self, table_name, data, database=None, columns=None, update=False, **kw):
        row_key = kw.pop('row_key', None)
        actions = []
        if columns:
            for d in data:
                info = {columns[i]: d[i] for i in range(len(columns))}
                action = {
                    '_index': kw.get('index', table_name),
                    '_type': table_name,
                }
                if update:
                    action['_op_type'] = 'update'
                    action['doc_as_upsert'] = True
                    action['doc'] = info
                else:
                    action['_source'] = info
                if row_key:
                    action['_id'] = action['_source'].get(row_key)
                actions.append(action)
        self.bulk(actions, **kw)

    def delete(self, table_name, database=None, **kw):
        self.cli.get()


class EsFactory(BaseDbFactory):
    instance_cls = Es
    db_type='Es'

    def get_db_conf(self, key: str) -> dict:
        return es_infos.get(key, dict())

    def db_confs(self) -> dict:
        return es_infos
