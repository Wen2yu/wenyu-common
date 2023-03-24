# -*- coding:utf-8 -*-

# Name: file_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/29 14:19

import linecache
import os

emp_str = ''


def read_str(path, size=-1, mode='r', encoding='utf-8'):
    """
    从文本文件中读取字符串
    :param path: 文本文件路径
    :param size: 读取大小，-1: 全量读取
    :param mode: 读取模式
    :param encoding: 文件编码
    :return: str
    """
    with open(path, mode=mode, encoding=encoding) as f:
        result = f.read()
    return result


def read_lines(path, mode='r', encoding='utf-8'):
    """
    从文件中读取每行内容，return list
    :param path: 文本文件路径
    :param mode: 读取模式
    :param encoding: 文件编码
    :return: list
    """
    with open(path, mode=mode, encoding=encoding) as f:
        result = f.readlines()
    return result


def get_line(path, lineno, module_globals=None):
    """
    Get the lines for a Python source file from the cache.
    :param path: 文件路径
    :param lineno: 行号
    :param module_globals:
    :return:
    """
    return linecache.getline(path, lineno=lineno, module_globals=module_globals)


def getlines(path, module_globals=None):
    """
    get the lines for a Python source file from the cache.
    Update the cache if it doesn't contain an entry for this file already.
    :param path:
    :param module_globals:
    :return:
    """
    return linecache.getlines(path, module_globals=module_globals)


def read_block(path, size=-1, mode='r', encoding='utf-8'):
    """
    分批从大文件中读取
    :param path: 文件路径
    :param size: 每次读取大小
    :param mode: 模式
    :param encoding: 编码
    :return: generator
    """
    with open(path, mode=mode, encoding=encoding) as f:
        while True:
            block = f.read(size)
            if not block:
                break
            yield block


def read_line(path, mode='r', encoding='utf-8'):
    """
    按行读取大文件
    :param path: 文件路径
    :param mode: 模式
    :param encoding: 编码
    :return: generator
    """
    with open(path, mode=mode, encoding=encoding) as f:
        for line in f:
            yield line


def write_file(path, data: str, mode='a+', encoding='utf-8'):
    """
    写入数据到文件
    :param path: 写入路径
    :param data: 待写入数据
    :param mode: 文件打开模式
    :param encoding: 文件编码
    :return:
    """
    with open(path, mode=mode, encoding=encoding) as f:
        if mode.find('b') > 1 and type(data) == str:
            data = data.encode(encoding=encoding)
        f.write(check_linesep(data))


def write_file_by_lines(path, lines, mode='a+', encoding='utf-8'):
    """
    写入多行数据到文件
    :param path: 写入路径
    :param lines: 待写入数据
    :param mode: 文件打开模式
    :param encoding: 文件编码
    :return:
    """
    with open(path, mode=mode, encoding=encoding) as f:
        if mode.find('b') > 1:
            lines = [
                check_linesep(line).encode(encoding=encoding) if type(line) == str else check_linesep(line)
                for line in lines
            ]
        f.writelines(lines=lines)


def check_linesep(line: str):
    """
    检查数据是否有换行符，如果没有则添加
    :param line:
    :return:
    """
    return f'{line}{os.linesep if line.endswith(os.linesep) else emp_str}'


def rmv_linesep(line: str, only_tail=1):
    """
    移除末尾的换行符
    :param line: 数据行
    :param only_tail: 只移除末尾
    :return:
    """
    if only_tail:
        return line.rstrip()
    else:
        return line.replace(os.linesep, '')
