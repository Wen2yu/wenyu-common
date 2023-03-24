# -*- coding: utf-8 -*-

"""
Name: DbClient
Product_name: PyCharm
Description: Database connection and operation classes.
Author: Cao Yitao
Date: 2021-06-16 17:57:47
"""

# //导入模块；
import cx_Oracle
import pymysql
from pyhive import hive
from impala.dbapi import connect
import configparser


class DbClient(object):
    """
    //数据库连接及操作类；
    """

    def __init__(self, **kwargs):
        self.db = kwargs.get("db", "")
        self.sid = kwargs.get("sid")
        self.kind = kwargs.get("kind")
        self.sql = kwargs.get("sql")
        self.data = kwargs.get("data")
        self.proc_name = kwargs.get("proc_name")
        self.proc_parm = kwargs.get("proc_parm")
        self.cfg = kwargs.get("cfg", "/u01/job/config/db_config.ini")

    def client_list(self):
        """
        //查询数据库地址列表方法；
        """
        cf = configparser.ConfigParser()
        cf.read(self.cfg)
        secs = cf.sections()
        res = [idx for idx in secs if idx.lower().startswith(self.db.lower())]
        for i in res:
            items = cf.items(i)
            print(str(i) + ": " + str(items))

    def orcl_client(self):
        """
        //Oracle连接方法；
        """
        try:
            # //获取库连接要素；
            cf = configparser.ConfigParser()
            cf.read(self.cfg)
            username = cf.get(self.sid, "username")
            password = cf.get(self.sid, "password")
            sid = cf.get(self.sid, "sid")
        except Exception as e:
            print(e)
        else:
            try:
                # //获取Oracle连接；
                db = cx_Oracle.connect(str(username) + "/" + str(password) + "@" + str(sid))
            except Exception as e:
                print(e)
            else:
                return db

    def mysql_client(self):
        """
        //MySQL连接方法；
        """
        try:
            # //获取库连接要素；
            cf = configparser.ConfigParser()
            cf.read(self.cfg)
            host = cf.get(self.sid, "host")
            user = cf.get(self.sid, "user")
            passwd = cf.get(self.sid, "passwd")
            db = cf.get(self.sid, "db")
        except Exception as e:
            print(e)
        else:
            try:
                # //获取MySQL连接；
                db = pymysql.connect(host=host, user=user, passwd=passwd, db=db)
            except Exception as e:
                print(e)
            else:
                return db

    def hive_client(self):
        """
        //Hive连接方法；
        """
        try:
            # //获取库连接要素；
            cf = configparser.ConfigParser()
            cf.read(self.cfg)
            host = cf.get(self.sid, "host")
            port = cf.get(self.sid, "port")
            username = cf.get(self.sid, "username")
            database = cf.get(self.sid, "database")
        except Exception as e:
            print(e)
        else:
            try:
                # //获取Hive连接；
                db = hive.Connection(host=host, port=port, username=username, database=database)
            except Exception as e:
                print(e)
            else:
                return db

    def impala_client(self):
        """
        //Impala连接方法；
        """
        try:
            # //获取库连接要素；
            cf = configparser.ConfigParser()
            cf.read(self.cfg)
            host = cf.get(self.sid, "host")
            port = cf.get(self.sid, "port")
        except Exception as e:
            print(e)
        else:
            try:
                # //获取Impala连接；
                db = connect(host=host, port=port)
            except Exception as e:
                print(e)
            else:
                return db

    def db_execute(self):
        """
        //数据库操作方法；
        """
        db_conn = ""
        try:
            # //根据数据库类型获取数据库连接；
            if self.db == "orcl":
                db_conn = self.orcl_client()
            elif self.db == "mysql":
                db_conn = self.mysql_client()
            elif self.db == "hive":
                db_conn = self.hive_client()
            elif self.db == "impala":
                db_conn = self.impala_client()
            else:
                pass
        except Exception as e:
            print(e)
        else:
            # //创建数据库cursor；
            db_cur = db_conn.cursor()

            # //返回数据库连接；
            if self.kind == "cursor":
                return db_conn
            # //查询操作；
            elif self.kind == "select":
                db_cur.execute(self.sql)
                result = db_cur.fetchall()
                return result
            # //插入操作；
            elif self.kind == "insert":
                db_cur.execute(self.sql)
                db_conn.commit()
            # //删除、更新操作；
            elif self.kind == "delete" and self.db == "orcl" or self.kind == "update" and self.db == "orcl":
                db_cur.execute(self.sql)
                db_conn.commit()
            # //清空操作；
            elif self.kind == "truncate" and self.db == "orcl":
                db_cur.execute(self.sql)
            # //批量插入操作；
            elif self.kind == "insert_many" and self.db == "orcl":
                dt = list(self.data)
                for i in range(0, len(dt), 5000):
                    d = dt[i:i + 5000]
                    db_cur.executemany(self.sql, d)
                    db_conn.commit()
            # //执行存储过程；
            elif self.kind == "proc" and self.db == "orcl":
                db_cur.callproc(self.proc_name, [self.proc_parm])
            else:
                pass
            # //关闭数据库连接；
            db_cur.close()
            db_conn.close()
