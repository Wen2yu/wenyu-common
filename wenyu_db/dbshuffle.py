# -*- coding: utf-8 -*-

"""
Name:           dbshuffle
Author:         Popeye
Description:    Common data processing tools.
"""

import re
import configparser
import datetime
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import six


def extract_table_name(sql):
    """
    从sql语句中提取所有数据库表名；
    :param sql: sql语句
    :return:    数据库表名
    """
    statement = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql)
    lines = [line for line in statement.splitlines() if not re.match("^\\s*(--|#)", line)]
    statement = " ".join([re.split("--|#", line)[0] for line in lines])
    elements = re.split(r"[\s)(;]+", statement)
    result = []
    step = False
    for e in elements:
        if step:
            if e.lower() not in ["", "select"]:
                result.append(e)
            else:
                pass
        step = e.lower() in ["from", "join"]
    return list(set(result))


def generate_insert(db, table, columns):
    """
    根据数据库表名和数据库列名生成insert语句；
    :param db:      数据库类型
    :param table:   数据库表名
    :param columns: 数据库列名
    :return:        insert语句
    """
    columns_list = columns.replace(" ", "").split(",")
    columns = columns.replace(" ", "").replace(",", ", ")
    if db.lower() == "orcl":
        values_template = ", ".join(":" + str(i) for i in range(1, len(columns_list) + 1))
    elif db.lower() == "mysql":
        values_template = ", ".join("%s" for i in range(1, len(columns_list) + 1))
    else:
        pass
    statement = f"insert into {table} ({columns}) values ({values_template})"
    return statement


def get_data_type(db, sid, table, column, author=None):
    """
    获取字段数据类型；
    :param db:      数据库类型
    :param sid:     数据库标签
    :param table:   数据库表名
    :param column:  数据库列名
    :param author:  脚本负责人
    :return:        字段数据类型
    """
    from dbbox.dbsuite import db_common
    result = None
    try:
        if db.lower() == "orcl":
            sql = f"select data_type from cols where table_name = upper('{table}') and column_name = upper('{column}')"
            result = db_common(db="orcl", sid=sid, way="select", sql=sql, author=author)
        elif db.lower() == "hive" or db.lower() == "impala":
            sql = f"describe {table}"
            df = db_common(db="impala", sid="cluster-cloud", way="read_sql", sql=sql, author=author)
            result = df[df['name'] == column]['type'].str.cat()
        elif db.lower() == "mysql":
            sql = f"SELECT data_type FROM information_schema.columns WHERE table_name = '{table}' AND column_name = '{column}' AND TABLE_SCHEMA = database()"
            result = db_common(db="mysql", sid=sid, way="select", sql=sql, author=author)
        else:
            pass
    except Exception as e:
        print(e)
    else:
        return result


def get_date_string(date, db):
    """
    根据时间格式生成格式化日期参数；
    :param date:    时间
    :param db:      数据库类型
    :return:        格式化日期参数
    """
    colon_count = re.findall(":", date)
    line_count = re.findall("-", date)
    len_date = len(date)
    string = None
    if str(db).lower() == "mysql":
        if len(line_count) == 2 and len(colon_count) == 2 and len_date == 19:
            string = "%Y-%m-%d %H:%M:%S"
        elif len(line_count) == 2 and len(colon_count) == 1 and len_date == 16:
            string = "%Y-%m-%d %H:%M"
        elif len(line_count) == 2 and len(colon_count) == 0 and len_date == 10:
            string = "%Y-%m-%d"
        elif len(line_count) == 0 and len(colon_count) == 2 and len_date == 17:
            string = "%Y%m%d %H:%M:%S"
        elif len(line_count) == 0 and len(colon_count) == 1 and len_date == 14:
            string = "%Y%m%d %H:%M"
        elif len(line_count) == 0 and len(colon_count) == 0 and len_date == 8:
            string = "%Y%m%d"
        else:
            pass
    elif str(db).lower() == "orcl":
        if len(line_count) == 2 and len(colon_count) == 2 and len_date == 19:
            string = "yyyy-mm-dd hh24:mi:ss"
        elif len(line_count) == 2 and len(colon_count) == 1 and len_date == 16:
            string = "yyyy-mm-dd hh24:mi"
        elif len(line_count) == 2 and len(colon_count) == 0 and len_date == 10:
            string = "yyyy-mm-dd"
        elif len(line_count) == 0 and len(colon_count) == 2 and len_date == 17:
            string = "yyyymmdd hh24:mi:ss"
        elif len(line_count) == 0 and len(colon_count) == 1 and len_date == 14:
            string = "yyyymmdd hh24:mi"
        elif len(line_count) == 0 and len(colon_count) == 0 and len_date == 8:
            string = "yyyymmdd"
        else:
            pass
    return string


def generate_truncate(table):
    """
    根据数据库表名生成truncate语句；
    :param table:   数据库表名
    :return:        truncate语句
    """
    sql = f"truncate table {table}"
    return sql


def generate_select(db, sid, table, partition=None, p_begin=None, p_end=None, timestamp=None, author=None):
    """
    根据数据库类型，数据库标签，数据库表名，增量、分区字段，增量、分区字段开始值，增量、分区字段结束值生成select语句；
    :param db:          数据库类型
    :param sid:         数据库标签
    :param table:       数据库表名
    :param partition:   增量、分区字段
    :param p_begin:     增量、分区字段开始值
    :param p_end:       增量、分区字段结束值
    :param timestamp:   是否是时间戳
    :param author:      脚本负责人
    :return:            select语句
    """
    sql = None
    if partition is not None:
        if len(np.array(partition, dtype=object).shape) > 0:
            partition = partition[0]

        if db.lower() == "orcl":
            data_type = get_data_type("orcl", sid, table, partition, author=author)
            if data_type:
                if data_type.upper() == "NUMBER":
                    sql = f"select count(*) from {table} where {partition} >= {p_begin} and {partition} < {p_end}"
                elif data_type.upper() == "DATE":
                    p_begin_string, p_end_string = get_date_string(p_begin, "orcl"), get_date_string(p_end, "orcl")
                    sql = f"select count(*) from {table} where {partition} >= to_date('{p_begin}', '{p_begin_string}') and {partition} < to_date('{p_end}', '{p_end_string}')"
            else:
                p_begin_string, p_end_string = get_date_string(p_begin, "orcl"), get_date_string(p_end, "orcl")
                sql = f"select count(*) from {table} where {partition} >= to_date('{p_begin}', '{p_begin_string}') and {partition} < to_date('{p_end}', '{p_end_string}')"
        elif db.lower() in ["hive", "impala"]:
            data_type = get_data_type("impala", sid, table, partition, author=author)
            if data_type.lower() == "int" or data_type.lower() == "bigint":
                sql = f"select count(*) from {table} where {partition} >= {p_begin} and {partition} < {p_end}"
            elif data_type.lower() == "string":
                sql = f"select count(*) from {table} where {partition} >= '{p_begin}' and {partition} < '{p_end}'"
        elif db.lower() == "mysql":
            data_type = get_data_type("mysql", sid, table, partition, author=author)
            if ("int" in data_type.lower() or data_type.lower() == "decimal" or data_type.lower() == "double" or data_type.lower() == "float") and timestamp is None:
                sql = f"select count(*) from {table} where {partition} >= {p_begin} and {partition} < {p_end}"
            elif "int" in data_type.lower() and timestamp is True:
                sql = f"select count(*) from {table} where {partition} >= unix_timestamp('{p_begin}') and {partition} < unix_timestamp('{p_end}')"
            elif data_type.lower() == "datetime" or data_type.lower() == "timestamp" or data_type.lower() == "date":
                sql = f"select count(*) from {table} where {partition} >= '{p_begin}' and {partition} < '{p_end}'"
            else:
                pass
        else:
            pass
    else:
        sql = f"select count(*) from {table}"
    return sql


def generate_delete(db, sid, table, partition, p_begin, p_end, author=None):
    """
    根据数据库标签，数据库表名，增量、分区字段，增量、分区字段开始值，增量、分区字段结束值生成delete语句；
    :param db:          数据库类型
    :param sid:         数据库标签
    :param table:       数据库表名
    :param partition:   增量、分区字段
    :param p_begin:     增量、分区字段开始值
    :param p_end:       增量、分区字段结束值
    :param author:      脚本负责人
    :return:            delete语句
    """
    sql = None
    data_type = get_data_type(db, sid, table, partition, author=author)
    if db.lower() == "orcl":
        if data_type.upper() == "NUMBER":
            sql = f"delete from {table} where {partition} >= {p_begin} and {partition} < {p_end}"
        elif data_type.upper() == "DATE":
            p_begin_string, p_end_string = get_date_string(p_begin, "orcl"), get_date_string(p_end, "orcl")
            sql = f"delete from {table} where {partition} >= to_date('{p_begin}', '{p_begin_string}') and {partition} < to_date('{p_end}', '{p_end_string}')"
        else:
            pass
    elif db.lower() == "mysql":
        if "int" in data_type.lower() or data_type.lower() == "decimal" or data_type.lower() == "double" or data_type.lower() == "float":
            sql = f"delete from {table} where {partition} >= {p_begin} and {partition} < {p_end}"
        elif data_type.lower() == "datetime" or data_type.lower() == "timestamp" or data_type.lower() == "date":
            sql = f"delete from {table} where {partition} >= '{p_begin}' and {partition} < '{p_end}'"
        else:
            pass
    return sql


def generate_hdfs_file_name(table, partition=None, extension=None):
    """
    根据数据库表名，目标数据库增量、分区字段，文件扩展名生成hdfs文件名；
    :param table:       数据库表名
    :param partition:   目标数据库增量、分区字段
    :param extension:   文件扩展名
    :return:            hdfs文件名
    """
    from kitbox.formatkit import now_timestamp
    table_name = table.replace(" ", "").split(".")[1]
    if extension is None:
        extension = ".txt"
    else:
        pass
    if partition is not None:
        string = '_'.join("{" + str(p).upper() + "}" for p in partition)
        hdfs_file_name = table_name + "_" + string + extension
    else:
        hdfs_file_name = table_name + "_" + str(now_timestamp()) + extension
    return hdfs_file_name


def generate_hdfs_folder(sid, table, partition=None):
    """
    根据数据库表名，目标数据库增量、分区字段，文件扩展名生成hdfs文件夹地址；
    :param sid:         数数据库标签
    :param table:       数据库表名
    :param partition:   目标数据库增量、分区字段
    :return:            hdfs文件夹地址
    """
    hdfs_config = "/u01/lab/config/hdfs_config.ini"
    cf = configparser.ConfigParser()
    cf.read(hdfs_config)
    dir_path = cf.get(sid, "dir_path")
    database = table.replace(" ", "").split(".")[0]
    table_name = table.replace(" ", "").split(".")[1]
    root_directory = dir_path.format(database=database) + f"{table_name}" + "/"
    if partition is not None:
        hdfs_folder = root_directory + '/'.join(str(k).lower() + "=" + str(v) for k, v in partition.items())
    else:
        hdfs_folder = root_directory
    return hdfs_folder


def data_slice(sid, data, table, partition, mode=None, sep=None):
    """
    根据增量、分区字段将数据切割，写入到对应本地文件；
    :param sid:         数据库标签
    :param data:        待插入数据
    :param table:       数据库表名
    :param partition:   增量、分区字段
    :param mode:        数据写入到对应本地文件的模式
    :param sep:         数据文件分隔符，默认为"|"
    :return:            本地文件字典
    """
    import sys
    from kitbox.formatkit import now_datetime
    cache_config = "/u01/lab/config/cache_config.ini"
    df = data
    # 设置数据写入到对应本地文件的模式；
    if mode is None:
        mode = "w"
    else:
        pass
    # 设置数据文件分隔符；
    if sep is None:
        sep = "|"
    else:
        pass
    # 定义本地文件字典；
    file_dict = {}
    # 定义本地文件路径；
    cf = configparser.ConfigParser()
    cf.read(cache_config)
    local_path = "{DIR_PATH}{FILE_NAME}"
    # 根据增量、分区字段将数据切割；

    if partition is not None:
        if isinstance(partition, str):
            partition = list(partition.replace(' ', '').split(','))
        partition = [ele.upper() if isinstance(ele, str) else ele for ele in partition]
        try:
            p_unique = df.drop_duplicates(subset=partition, keep='first')[partition].values
        except Exception as e:
            print("【{NOW}】 增量分区字段Error: {ERROR}".format(NOW=now_datetime(), ERROR=str(e)))
            sys.exit(0)

        command = "df[" + " & ".join('df["' + str(p).upper() + '"].isin([u[' + str(i - 1) + "]])" for p, i in
                                     zip(partition, [i for i in range(1, len(partition) + 1)])) + "]"
        # print(command)
        for u in p_unique:
            hdfs_folder = generate_hdfs_folder(sid, table, dict(zip(partition, u)))
            p_df = eval(command)
            print(p_df)
            string = ', '.join(p + "=u[" + str(i) + "]" for p, i in zip(partition, range(0, len(partition))))
            format_string = f"generate_hdfs_file_name(table, partition).format({string})"
            # 数据写入到对应本地文件；
            try:
                p_df.to_csv(local_path.format(DIR_PATH=cf.get("cache_file", "dir_path"), FILE_NAME=eval(format_string)), index=False, header=False, sep=sep, mode=mode)
            except Exception as e:
                print("【{NOW}】 数据写入Error: {ERROR}".format(NOW=now_datetime(), ERROR=str(e)))
                sys.exit(0)
            else:
                file_dict[hdfs_folder] = local_path.format(DIR_PATH=cf.get("cache_file", "dir_path"),
                                                           FILE_NAME=eval(format_string))
    else:
        local_path = local_path.format(DIR_PATH=cf.get("cache_file", "dir_path"),
                                       FILE_NAME=generate_hdfs_file_name(table))
        df.to_csv(local_path, index=False, header=False, sep=sep, mode=mode)
        hdfs_folder = generate_hdfs_folder(sid, table)
        file_dict[hdfs_folder] = local_path

    NOW = now_datetime()
    print(f"【{NOW}】 根据增量分区字段切割文件完成.")
    return file_dict


def reverse_dictionary(d):
    """
    反转字典键值对；
    :param d:       待反转字典
    :return:        反转后字典
    """
    return dict([(v, k) for (k, v) in d.items()])


def reverse_str(char):
    """
    反转字符串；
    :param char:    待反转字符串
    :return:        反转后字符串
    """
    if isinstance(char, str):
        rev_char = ''.join(reversed(char))
        return rev_char
    else:
        return None


def cut_last_underline(char):
    """
    截取最后一个下划线后面的数据；
    :param char:    待截取字符串
    :return:        截取后字符串
    """
    return char.split("_")[-1] if isinstance(char, str) else char


def get_list_diff_48(seq, list_rs):
    """
    根据最近一个半点的序号，对比结果表的序号与之的差集；
    :param seq:         最近一个半点序号
    :param list_rs:     结果表的序号
    :return:            对比后的差集
    """
    now_seq_list = list(range(1, seq + 1))
    if list_rs:
        rs_seq_list = [i[0] for i in list_rs]
    else:
        rs_seq_list = []
    return list(set(now_seq_list).difference(set(rs_seq_list)))


def db_convert_list(gather):
    """
    将数据库查询结果提取出合并到list；
    :param gather:      数据库查询结果的集合
    :return:            合并后的list
    """
    if gather:
        result = [i[0] for i in gather]
    else:
        result = []
    return result


def union_list(*args):
    """
    获取传入列表的并集；
    :param args:        传入的列表集合
    :return:            传入的列表集合并集
    """
    result = []
    for i in args:
        result = list(set(result).union(set(i)))
    return result


def df_set_tolist(dataframe, column):
    """
    dataframe某一列转列表并排重
    :param dataframe:       dataframe
    :param column:          待处理列名
    :return:                列转列表并排重
    """
    listview = dataframe[column].values.tolist()
    listview_idx = list(set(listview))
    listview_idx.sort()
    return listview_idx


def df_duplicates(dataframe, *args):
    """
    dataframe选取某几列进行排重，生成新的dataframe选取某几列进行排重
    :param dataframe:       dataframe
    :param args:            选取的列名集合
    :return:                排重后新的dataframe
    """
    df = dataframe.drop_duplicates(subset=list(args), keep='first', inplace=False)
    df = df[list(args)].reset_index(drop=True)
    return df


def str_width(string):
    """
    根据字符串判断列宽长度；
    :param string:              字符串
    :return:                    列宽长度
    """
    str_list = list(str(string))
    width = 0
    for char in str_list:
        # 判断是否是汉字；
        if u'\u4e00' <= char <= u'\u9fa5':
            width += 1.7
        # 判断是否是数字；
        elif char.isdigit():
            width += 0.9
        # 判断是否是大写字母；
        elif char.isupper():
            width += 1.5
        # 判断是否是小写字母；
        elif char.islower():
            width += 1
        else:
            width += 0.9
    return width


def df_width(dataframe, scale):
    """
    dataframe计算每列最长字符串长度，并根据传入单位返回每列宽度；
    :param dataframe:       dataframe
    :param scale:           单位
    :return:                dataframe每列宽度
    """
    data = dataframe.copy()
    data = pd.DataFrame(np.insert(data.values, 0, values=data.columns, axis=0))
    width = []
    for col_num, value in enumerate(data.columns.values):
        width.append(int(data[value].map(lambda x: str_width(str(x))).max()) / 5 * scale)
    print(width)
    return width


def render_mpl_table(data, col_width=3.0, row_height=0.625, font_size=14, header_color='#40466e', row_colors=None, edge_color='w', bbox=None, header_columns=0, ax=None, cell_width=None, **kwargs):
    """
    将dataframe转成表格；
    :param data:                dataframe
    :param col_width:           列宽，用于计算画布大小
    :param row_height:          行高，用于计算画布大小
    :param font_size:           字体大小
    :param header_color:        表头颜色
    :param row_colors:          数据行颜色
    :param edge_color:          边的颜色
    :param bbox:                边界框
    :param header_columns:      设置为表头的列，小于等于该值的都被设置为表头颜色
    :param ax:                  ax对象元组
    :param cell_width:          表格列宽
    :return:                    表格
    """
    plt.rcParams["font.sans-serif"] = ["SimHei"]
    plt.rcParams["axes.unicode_minus"] = False

    if cell_width is None:
        cell_width = df_width(data, kwargs.get("scale", 0.1))
        coef = np.sum(np.array(cell_width))

    else:
        coef = 1

    if row_colors is None:
        row_colors = ['#f1f1f2', 'w']

    if bbox is None:
        bbox = [0, 0, 1, 1]

    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([coef * col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')
    mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, cellLoc="center", **kwargs)
    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)
    for k, cell in six.iteritems(mpl_table._cells):
        if cell_width is None:
            pass
        else:
            cell.set_width(cell_width[k[1]])
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0] % len(row_colors)])
    return ax