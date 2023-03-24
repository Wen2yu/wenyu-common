# -*- coding:utf-8 -*-

# Name: hdfs
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/10/28 15:19

import re
import traceback
from hdfs import *

from jy_conf import hdfs_infos


class Hdfs:
    client = None

    def __init__(self, key: str = None, **kw):
        conf = dict()
        if key and hdfs_infos.get(key):
            conf.update(hdfs_infos[key])
        conf.update(kw)
        urls = conf.pop('urls')
        if urls:
            for url in urls:
                try:
                    self.client = InsecureClient(url, **conf)
                    break
                except Exception:
                    continue

    def list(self, hdfs_path, status=False):
        return self.client.list(hdfs_path, status=status)

    def write(self, hdfs_path, data, **kw):
        return self.client.write(hdfs_path, data, **kw)

    def upload(self, hdfs_path, local_path, **kw):
        return self.client.upload(hdfs_path, local_path, **kw)

    def rename(self, hdfs_src_path, hdfs_dst_path, **kw):
        return self.client.rename(hdfs_src_path=hdfs_src_path, hdfs_dst_path=hdfs_dst_path)

    def delete(self, hdfs_path: str, recursive=False, skip_trash=True):
        if hdfs_path.find('*') > 0:
            idx = hdfs_path.rfind('/')
            path, filename_pat = hdfs_path[:idx+1], re.compile(hdfs_path[idx+1:].replace('*', '.*'))
            try:
                for filename in self.list(path):
                    if filename_pat.match(filename):
                        if not self.client.delete(path+filename, recursive=recursive, skip_trash=skip_trash):
                            return False
            except Exception as e:
                traceback.print_exc()
                print(e)
                return False
        else:
            return self.client.delete(hdfs_path, recursive=recursive, skip_trash=skip_trash)
