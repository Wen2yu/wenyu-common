# -*- coding: utf-8 -*-

"""
Name:           dbmirror
Author:         Popeye
Description:    Data synchronization and migration module.
"""

# 导入模块
import inspect
import sys
import time
import pandas as pd
# from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor
from dbbox.dbdecorator import data_volume
from dbbox.dbopen import DbOpen
from dbshuffle import generate_insert, generate_delete, generate_truncate, data_slice
from kitbox.formatkit import now_datetime
from kitbox.decodekit import *
import os
from kitbox.txtkit import write_log
from kitbox.urlkit import alarm_report


# 定义类
class DbMirror(object):
    def __init__(self, **kwargs):
        """
        实例化参数；
        :param src:     源数据参数
        :param tgt:     目标数据参数
        :param conv:    源数据转换参数
        :param wx:      微信监控参数
        :param error:   异常信息打印内容
        """
        self.src = kwargs.get("src", None)
        self.tgt = kwargs.get("tgt", None)
        self.conv = kwargs.get("conv", None)
        self.wx = kwargs.get("wx", None)
        self.error = "[{NOW}][{FILE}][{FUNCTION}]Error: {ERROR}"
        self.error_file = "/u01/lab/logs/dbmirror_error.log"
        self.result_list = []
        self.author = self.wx.get("receiver", None) if self.wx is not None else None

    def src_param(self):
        """
        源数据参数整合；
        :return:        源数据参数整合列表
        """
        src_param_list = []
        for src in self.src:
            src_param_dict = {"db": src.get("db", "").lower() if src.get("db") is not None and isinstance(src.get("db"), str) else src.get("db", None),
                              "sid": src.get("sid", None),
                              "sql": src.get("sql", None),
                              "log": src.get("log", None),
                              "scale": src.get("scale", None)}
            src_param_list.append(src_param_dict)
        return src_param_list

    def tgt_param(self, data):
        """
        目标数据参数整合；
        :param data:    待插入数据
        :return:        目标数据参数整合列表
        """
        tgt_param_list = []
        for tgt in self.tgt:
            tgt_param_dict = {"db": tgt.get("db").lower() if tgt.get("db") is not None and isinstance(tgt.get("db"), str) else tgt.get("db", None),
                              "sid": tgt.get("sid"),
                              "table": tgt.get("table"),
                              "columns": tgt.get("columns",
                                                 ",".join(str(x).lower() for x in data.columns.tolist()) if tgt.get("db").lower() in ["orcl", "mysql"] and isinstance(
                                                     tgt.get("db"), str) else None),
                              "sql": generate_insert(tgt.get("db").lower() if tgt.get("db") is not None and isinstance(tgt.get("db"), str) else tgt.get("db", None), tgt.get("table"),
                                                     tgt.get("columns", ",".join(
                                                         str(x).lower() for x in data.columns.tolist()) if tgt.get("db").lower() in ["orcl", "mysql"] and isinstance(tgt.get("db"),
                                                                                                                                                                     str) else None)) if tgt.get(
                                  "db").lower() in ["orcl", "mysql"] and isinstance(tgt.get("db"), str) else None,
                              "overwrite": tgt.get("overwrite").lower() if tgt.get("overwrite") is not None and isinstance(tgt.get("overwrite"), str) else tgt.get("overwrite", "n"),
                              "partition": tgt.get("partition", None),
                              "p_begin": tgt.get("p_begin", None),
                              "p_end": tgt.get("p_end", None),
                              "receiver": self.wx.get("receiver", None) if self.wx is not None else None,
                              "sep": tgt.get("sep", None)}
            tgt_param_list.append(tgt_param_dict)
        return tgt_param_list

    def src_extract(self, db, sid, sql, log, scale=None):
        """
        源数据抽取方法；
        :param db:      数据库类型
        :param sid:     数据库标签
        :param sql:     执行语句
        :param log:     日志文件路径
        :param scale:   小数保留位数
        :return:        返回数据结果
        """
        d = DbOpen(db=db, sid=sid, way="read_sql", sql=sql, log=log, author=self.author, scale=scale)
        result = d.db_open()
        result.rename(columns=lambda x: x.upper(), inplace=True)
        # 日志输出；
        print("【{NOW}】 {db}:{sid} 数据读取成功.".format(NOW=now_datetime(), db=db, sid=sid))
        # 退出程序；
        return result

    def mysql_remove(self, sid, table, overwrite, partition=None, p_begin=None, p_end=None):
        """
        MySQL目标数据库清除方法；
        :param sid:         数据库标签
        :param table:       数据库表名
        :param overwrite:   目标数据库写入方式
        :param partition:   目标数据库增量、分区字段
        :param p_begin:     目标数据库增量、分区字段开始值
        :param p_end:       目标数据库增量、分区字段结束值
        """
        # 清空全表；
        if str(overwrite).lower() == "y":
            try:
                d = DbOpen(db="mysql", sid=sid, way="ddl", sql=generate_truncate(table), author=self.author)
                d.db_open()
                # 日志输出；
                print("【{NOW}】 mysql:{sid} 数据清空成功.".format(NOW=now_datetime(), sid=sid))
            except Exception as e:
                # 异常输出；
                print(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)))
                write_log(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)), self.error_file)
                # 退出程序；
                sys.exit(0)

        # 根据增量字段删除数据；
        elif str(overwrite).lower() == "n" and partition is not None:
            sql = generate_delete("mysql", sid, table, partition, p_begin, p_end)
            print(sql)
            try:
                d = DbOpen(db="mysql", sid=sid, way="dml", sql=sql, author=self.author)
                d.db_open()
                # 日志输出；
                print("【{NOW}】 mysql:{sid} 数据删除成功.".format(NOW=now_datetime(), sid=sid))
            except Exception as e:
                # 异常输出；
                print(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)))
                write_log(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)), self.error_file)
                # 退出程序；
                sys.exit(0)

    @data_volume(db="mysql")
    def mysql_replicate(self, sid, sql, data, table=None, partition=None, p_begin=None, p_end=None, receiver=None):
        """
        MySQL目标数据库写入方法；
        :param sid:         数据库标签
        :param sql:         执行语句
        :param data:        待插入数据
        :param table:       数据库表名
        :param partition:   目标数据库增量、分区字段
        :param p_begin:     目标数据库增量、分区字段开始值
        :param p_end:       目标数据库增量、分区字段结束值
        :param receiver:    收信人
        """
        if data.shape[0] > 0:
            if isinstance(data, pd.DataFrame):
                data = data.values.tolist()
            try:
                d = DbOpen(db="mysql", sid=sid, way="insert_many", sql=sql, data=data, author=self.author)
                d.db_open()
            except Exception as e:
                # 异常输出；
                print(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)))
                write_log(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)), self.error_file)
            else:
                # 日志输出；
                print("【{NOW}】 mysql:{sid} 数据写入成功.".format(NOW=now_datetime(), sid=sid))
        else:
            # 日志输出；
            print("【{NOW}】 mysql:{sid} 待插入数据量为0，跳过插入步骤.".format(NOW=now_datetime(), sid=sid))

    def orcl_remove(self, sid, table, overwrite, partition=None, p_begin=None, p_end=None):
        """
        Oracle目标数据库清除方法；
        :param sid:         数据库标签
        :param table:       数据库表名
        :param overwrite:   目标数据库写入方式
        :param partition:   目标数据库增量、分区字段
        :param p_begin:     目标数据库增量、分区字段开始值
        :param p_end:       目标数据库增量、分区字段结束值
        """
        # 清空全表；
        if str(overwrite).lower() == "y":
            try:
                d = DbOpen(db="orcl", sid=sid, way="ddl", sql=generate_truncate(table), author=self.author)
                d.db_open()
                # 日志输出；
                print("【{NOW}】 orcl:{sid} 数据清空成功.".format(NOW=now_datetime(), sid=sid))
            except Exception as e:
                # 异常输出；
                print(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)))
                write_log(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)), self.error_file)
                # 退出程序；
                sys.exit(0)

        # 根据增量字段删除数据；
        elif str(overwrite).lower() == "n" and partition is not None:
            sql = generate_delete("orcl", sid, table, partition, p_begin, p_end)
            try:
                d = DbOpen(db="orcl", sid=sid, way="dml", sql=sql, author=self.author)
                d.db_open()
                # 日志输出；
                print("【{NOW}】 orcl:{sid} 数据删除成功.".format(NOW=now_datetime(), sid=sid))
            except Exception as e:
                # 异常输出；
                print(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)))
                write_log(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)), self.error_file)
                # 退出程序；
                sys.exit(0)

    @data_volume(db="orcl")
    def orcl_replicate(self, sid, sql, data, table=None, partition=None, p_begin=None, p_end=None, receiver=None):
        """
        Oracle目标数据库写入方法；
        :param sid:         数据库标签
        :param sql:         执行语句
        :param data:        待插入数据
        :param table:       数据库表名
        :param partition:   目标数据库增量、分区字段
        :param p_begin:     目标数据库增量、分区字段开始值
        :param p_end:       目标数据库增量、分区字段结束值
        :param receiver:    收信人
        """

        if data.shape[0] > 0:
            if isinstance(data, pd.DataFrame):
                data = data.values.tolist()
            try:
                d = DbOpen(db="orcl", sid=sid, way="insert_many", sql=sql, data=data, author=self.author)
                d.db_open()
            except Exception as e:
                # 异常输出；
                print(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)))
                write_log(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)), self.error_file)
            else:
                # 日志输出；
                print("【{NOW}】 orcl:{sid} 数据写入成功.".format(NOW=now_datetime(), sid=sid))
        elif data.shape[0] == 0:
            print("【{NOW}】 orcl:{sid} 待插入数据为0.".format(NOW=now_datetime(), sid=sid))

    def hive_remove(self, sid, hdfs_folder):
        """
        Hive目标数据库清除方法；
        :param sid:         数据库标签
        :param hdfs_folder: hdfs文件夹地址
        :return:
        """
        try:
            d = DbOpen(db="hdfs", way="cursor", sid=sid, author=self.author)
            db_conn = d.db_open()
        except Exception as e:
            # 异常输出；
            print(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)))
            write_log(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)), self.error_file)
        else:
            db_conn.delete(hdfs_folder, recursive=True)
            # 日志输出；
            print("【{NOW}】 hdfs:{sid} 数据清除成功.".format(NOW=now_datetime(), sid=sid))

    @data_volume(db="impala")
    def hive_replicate(self, sid, table, data, overwrite, partition=None, p_begin=None, p_end=None, receiver=None, sep=None):
        """
        Hive目标数据库写入方法；
        :param sid:         数据库标签
        :param table:       数据库表名
        :param data:        待插入数据
        :param overwrite:   目标数据库写入方式
        :param partition:   目标数据库增量、分区字段
        :param p_begin:     目标数据库增量、分区字段开始值
        :param p_end:       目标数据库增量、分区字段结束值
        :param receiver:    收信人
        :param sep:         数据文件分隔符，默认为"|"
        :return:
        """
        # 根据待插入数据，目标数据库增量、分区字段将数据切割，写入到对应本地文件；
        file_dict = data_slice(sid, data, table, partition, sep=sep)
        print(file_dict)
        # 循环上传文件到hdfs;
        for k, v in file_dict.items():
            if str(overwrite).lower() == "y":
                # 清除hdfs文件夹地址；
                self.hive_remove(sid, k)
            else:
                pass
            try:
                d = DbOpen(db="hdfs", way="cursor", sid=sid, author=self.author)
                db_conn = d.db_open()
            except Exception as e:
                # 异常输出；
                print(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)))
                write_log(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)), self.error_file)
            else:
                for i in range(20):
                    try:
                        # hdfs文件夹地址不存在则创建；
                        if db_conn.status(k, strict=False) is None:
                            db_conn.makedirs(k)
                        db_conn.upload(k, v, overwrite=True)
                        # 日志输出；
                        print("【{NOW}】 {k}:{v} 数据文件上传成功.".format(NOW=now_datetime(), k=k, v=v))
                    except Exception as e:
                        # 异常输出；
                        print(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)))
                        write_log(self.error.format(NOW=now_datetime(), FILE=str(os.path.abspath(sys.argv[0])), FUNCTION=inspect.stack()[0][3], ERROR=str(e)), self.error_file)
                        alarm_report(error=e, author=self.author)
                        time.sleep(60)
                    else:
                        break

        # 修复分区信息；
        if partition is not None:
            d = DbOpen(db="hive", way="ddl", sid=sid, sql=f"msck repair table {table}", author=self.author)
            d.db_open()
            # 日志输出；
            print("【{NOW}】 hive:{sid} {table}分区修复成功.".format(NOW=now_datetime(), sid=sid, table=table))
        else:
            pass

    def db_conv(self, data):
        """
        源数据转换方法；
        :param data:      待插入数据
        :return:          转换后数据
        """

        for k, v in self.conv.items():
            if isinstance(v, list):
                for c in v:
                    data[c.upper()] = data[c.upper()].map(lambda x: eval(k + "(x)"))
                    # 日志输出；
                    print("【{NOW}】 [{k}][{c}] 数据转换完成.".format(NOW=now_datetime(), k=k, c=c))
            else:
                data[v.upper()] = data[v.upper()].map(lambda x: eval(k + "(x)"))
                # 日志输出；
                print("【{NOW}】 [{k}][{v}] 数据转换完成.".format(NOW=now_datetime(), k=k, v=v))
        return data

    def db_callback(self, future):
        """
        配合多进程进行数据合并的回调函数；
        :return:
        """
        res = future.result()
        self.result_list.append(res)

    def db_mirror(self):
        """
        数据迁移调度方法；
        :return:
        """
        # 日志输出；
        print("【{NOW}】 数据同步开始".center(60, "=").format(NOW=now_datetime()))

        # 源数据读取操作；
        src_param_list = self.src_param()

        # 判断源数据参数中数据库类型是否正确；
        if len(src_param_list) > 0:
            for src_param in src_param_list:
                if src_param.get("db") not in ["orcl", "mysql", "hive", "impala", "hdfs"]:
                    # 日志输出；
                    print("【{NOW}】 {db}不支持的数据库类型.".format(NOW=now_datetime(), db=src_param.get("db")))
                    # 退出程序；
                    sys.exit(0)
                else:
                    pass
        else:
            # 日志输出；
            print("【{NOW}】 源数据参数为空.".format(NOW=now_datetime()))
            # 退出程序；
            sys.exit(0)

        # 日志输出；
        print("【{NOW}】 源数据参数: ".format(NOW=now_datetime()), src_param_list)

        # 定义变量接收源数据；
        data = pd.DataFrame()

        # 定义多进程读取源数据；
        p = ProcessPoolExecutor(len(src_param_list))
        for src in src_param_list:
            future = p.submit(self.src_extract, src.get("db"), src.get("sid"), src.get("sql"), src.get("log"), src.get("scale"))
            future.add_done_callback(self.db_callback)
        print("Src ProcessPoolExecutor Closing.")
        p.shutdown(wait=True)
        print("Src ProcessPoolExecutor Done.")
        for rl in self.result_list:
            data = data.append(rl)
        rows = data.shape[0]
        # 日志输出；
        print("【{NOW}】 所有数据读取完成, ".format(NOW=now_datetime()), rows, "rows")
        # 打印所有数据；
        print(data)

        if int(rows) > 0:
            # 源数据处理NaN类型；
            data = data.astype(object).where(pd.notnull(data), None)

            # 源数据转换操作；
            if self.conv is not None and data.shape[0] > 0:
                data = self.db_conv(data)
                print(data)
            else:
                pass

            # 目标数据清除操作；
            tgt_param_list = self.tgt_param(data)
            # 日志输出；
            print("【{NOW}】 目标数据参数: ".format(NOW=now_datetime()), tgt_param_list)

            for tgt in tgt_param_list:
                if tgt.get("db") == "orcl" and (
                        tgt.get("overwrite") == "y" or tgt.get("overwrite") == "n" and tgt.get("partition") is not None):
                    self.orcl_remove(tgt.get("sid"), tgt.get("table"), tgt.get("overwrite"), tgt.get("partition"),
                                     tgt.get("p_begin"), tgt.get("p_end"))
                elif tgt.get("db") == "mysql" and (
                        tgt.get("overwrite") == "y" or tgt.get("overwrite") == "n" and tgt.get("partition") is not None):
                    self.mysql_remove(tgt.get("sid"), tgt.get("table"), tgt.get("overwrite"), tgt.get("partition"),
                                      tgt.get("p_begin"), tgt.get("p_end"))
                else:
                    pass

            # 目标数据写入操作；
            # 定义多进程写入源数据；
            p = ProcessPoolExecutor(len(tgt_param_list))
            for tgt in tgt_param_list:
                if tgt.get("db") == "orcl":
                    p.submit(self.orcl_replicate, sid=tgt.get("sid"), sql=tgt.get("sql"), data=data,
                             table=tgt.get("table"), partition=tgt.get("partition"),
                             p_begin=tgt.get("p_begin"),
                             p_end=tgt.get("p_end"),
                             receiver=tgt.get("receiver"))
                elif tgt.get("db") == "mysql":
                    p.submit(self.mysql_replicate, sid=tgt.get("sid"), sql=tgt.get("sql"), data=data,
                             table=tgt.get("table"), partition=tgt.get("partition"),
                             p_begin=tgt.get("p_begin"),
                             p_end=tgt.get("p_end"),
                             receiver=tgt.get("receiver"))
                elif tgt.get("db") == "hive":
                    p.submit(self.hive_replicate, sid=tgt.get("sid"), table=tgt.get("table"), data=data,
                             overwrite=tgt.get("overwrite"), partition=tgt.get("partition"),
                             p_begin=tgt.get("p_begin"),
                             p_end=tgt.get("p_end"), receiver=tgt.get("receiver"), sep=tgt.get("sep"))
                else:
                    # 日志输出；
                    print("【{NOW}】 {db}不支持的数据库类型.".format(NOW=now_datetime(), db=tgt.get("db")))
            print("Tgt ProcessPoolExecutor Closing.")
            p.shutdown(wait=True)
            print("Tgt ProcessPoolExecutor Done.")

        # 日志输出；
        print("【{NOW}】 数据同步结束".center(60, "=").format(NOW=now_datetime()))