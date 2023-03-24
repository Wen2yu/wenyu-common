# -*- coding:utf-8 -*-

# Name: jydata_crypt
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/11/9 14:23


b, key_str, enchar = 21, 'zfukwpaxgjelqmocvbtyd', '0123456789._-+xX=,abc'


def encode(src_str):
    """
    佳缘数据中心加密
    :param src_str:待加密字符串
    :return:
    """
    var2, var4, var8, result = len(src_str), 0, 0, ''
    for i in range(var2):
        var8 = enchar.find(src_str[i])
        if var8 == - 1:
            result += src_str[i]
            var8 = 0
        else:
            var4 = (var8 + i * (i+7+len(src_str))*var4) % b
            result += key_str[var4]
        var4 = var8
    return result


def decode(enc_str):
    """
    佳缘数据中心解密
    :param enc_str: 待解密字符串
    :return:
    """
    var2, var4, var8, result = len(enc_str), 0, 0, ''
    for i in range(var2):
        var8 = key_str.find(enc_str[i])
        if var8 == -1:
            result += enc_str[i]
            var8 = 0
        else:
            var8 = ((var8 - i * (i + 7 + len(enc_str)) * var4) % b + b) % b
            result += enchar[var8]
        var4 = var8
    return result
