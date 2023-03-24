# -*- coding:utf-8 -*-

# Name: str_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/17 19:08

from .sys_utils import str_val


def zip_str(s: str, reverse=False):
    """
    将给定字符串打包成dict(), k, v分别为位置索引和对应的字符
    :param s:
    :param reverse: 书否反转
    :return:
    """
    return {i: s[-i - 1 if reverse else i] for i in range(0, len(s))}


def sorted_str(s: str, idxes=None, reverse=False):
    """
    字符串内部排序， 返回排序后的字符串
    :param s:
    :param idxes: 给定的索引顺序
    :param reverse: 是否反转
    :return:
    """
    return ''.join([s[int(i)] for i in (reversed(idxes) if reverse else idxes)] if idxes else sorted(s, reverse=reverse))


def str2dict(s: str, sps='&=', keys: list = None) -> dict:
    """
    把有分隔符的字符串转换成dict
    :param s: 输入字符串
    :param sps: 分隔符
    :param keys: 给定key集合，在没有二级分隔符(sp)时，将一级分割结果依次匹配key
    :return: 返回分割成的dict
    """
    s = str_val(s)
    if type(s) == dict:
        return s
    result, len_keys = dict(), len(keys) if keys else 0
    if not sps:
        sps = [None]
    kvs = str(s).split(sps[0])
    for i in range(0, len(kvs)):
        if kvs[i]:
            if len_keys > i:
                result[keys[i]] = str2dict(kvs[i], sps[1:]) if len(sps) > 1 and kvs[i].count(sps[1]) > 0 else kvs[i]
            elif len(sps) > 1:
                idx_split = kvs[i].find(sps[1])
                k, v = kvs[i][:idx_split], kvs[i][idx_split+1:]
                try:
                    result[k] = eval(v) if v not in ['id'] else v
                except Exception:
                    result[k] = v
    return result


def str2values(s, split=''):
    """

    :param s: 源字符串
    :param split: 分隔符
    :return:
    """
    result = []
    values = s.split(split) if split else [s]
    for value in values:
        try:
            value = eval(value)
        except:
            pass
        result.append(value)
    return tuple(result)


def substr_by_chars(s: str, chars='', include_chars=0):
    """
    根据给定字符集截取字符串
    :param s: 原始字符
    :param chars: 给定字符集
    :param include_chars: 结果是否包含给定字符集: 0:不包含；1：包含左侧； 2：包含右侧； 3：包含两侧, 其他值等价于1
    :return:
    """
    result = []
    idx1 = idx2 = 0
    if chars:
        for char in chars:
            if idx1 > idx2:
                idx2 = s.index(char, idx1) + (include_chars in [2, 3])
            else:
                idx1 = s.find(char, idx2) + (include_chars in [0, 2])
            if idx2:
                result.append(s[idx1:idx2])
        if idx2 == 0:
            return s[idx1:]
        return result[0] if len(result) == 1 else result
    else:
        return s
