# -*- coding: utf-8 -*-

# Name: text_utils
# Product_name: PyCharm
# Description:
# Author: 'caoyitao'
# Date: 2021-06-16 17:57:47

import jieba
import pypinyin
import re

wn_dict = {
    'yi': '1', 'one': '1', 'first': '1', '1': '1', '①': '1', 'Ⅰ': '1',
    'er': '2', 'two': '2', 'to': '2', 'second': '2', '2': '2', '②': '2', 'Ⅱ': '2',
    'san': '3', 'three': '3', 'tree': '3', 'third': '3', 'can': '3', '3': '3', '③': '3', 'Ⅲ': '3',
    'si': '4', 'four': '4', '4': '4', '④': '4', 'Ⅳ': '4',
    'wu': '5', 'five': '5', '5': '5', '⑤': '5', 'Ⅴ': '5',
    'liu': '6', 'six': '6', 'lu': '6', '6': '6', '⑥': '6', 'Ⅵ': '6',
    'qi': '7', ', seven': '7', '7': '7', '⑦': '7', 'Ⅶ': '7',
    'ba': '8', 'eight': '8', '8': '8', '⑧': '8', 'Ⅷ': '8',
    'jiu': '9', 'nine': '9', '9': '9', '⑨': '9', 'Ⅸ': '9',
    'ling': '0', 'zero': '0', 'ten': '0', 'leng': '0', '0': '0', 'O': '0', 'o': '0', '⑩': '0', 'Ⅹ': '0', '〇': '0',
    'weixin': 'WX', 'wei': 'W', 'xin': 'X', '♥': 'X', 'q': 'Q', 'Q': 'Q', 'kou': 'Q',
    'mobile': '手机', 'tel': '手机', 'tele': '手机', 'sj': '手机', 'sjh': '手机', 'sjhm': '手机',
    'shou': '手', 'ji': '机',
}
re_dict = {
    '手机': re.compile('(?:手机)?号?[码吗]?[:：—_|\s]{0,3}([+]?\d{0,3}1\d{10})[:：—_|\s]{0,3}.{0,6}(?:手机)?号?[码吗]?'),
    '微信': re.compile('W.{0,3}X[:：—_|\s]{0,3}([A-Za-z0-9_-]{7,14})'),
    '微信1': re.compile('([A-Za-z0-9_-]{7,})[:：—_|\s]{0,3}.{0,6}W.{0,3}X'),
    'QQ': re.compile('Q{0,2}[:：—_|\s]{0,3}(\d{7,11})'),
    'QQ1': re.compile('(\d{7,11})[:：—_|\s]{0,3}.{0,6}Q{0,2}'),
}


# 定义函数
def write_text(data, file, split="|"):
    """
    //数据导出写入文本文件；
    //可自定义分隔符，默认|分割；
    """
    f = open(file, 'w').close()
    f = open(file, 'a')
    for t in data:
        f.write(split.join(str(i) for i in t) + "\n")
    f.close()


def withdraw_nums(text, retail_lf=True):
    """

    :param text:
    :param retail_lf: 是否保留换行符
    :return:
    """
    nums, _text = [], []
    words = list(jieba.cut(text, cut_all=True))
    for w in words:
        if wn_dict.get(w):
            nums.append(wn_dict[w])
            w = w.replace(w, '*')
        else:
            for s in w:
                p = pypinyin.pinyin(s, style=pypinyin.NORMAL)[0][0]
                nums.append(wn_dict.get(p.lower(), s))
                if wn_dict.get(p.lower()):
                    w = w.replace(s, '*')
        _text.append(w)
    return ''.join(nums), ''.join(_text)


def replace_contact(text):
    return withdraw_nums(text)[1]


def withdraw_contact(text):
    nums = withdraw_nums(text)[0]
    for contact_type, pat in re_dict.items():
        contacts = pat.findall(nums)
        if contacts:
            for contact in contacts:
                nums = nums.replace(contact, '*' * len(contact))
        yield contact_type, contacts


def contain_contacts(text):
    infos = withdraw_contact(text)
    for (contact_type, contacts) in infos:
        if contacts:
            return True
        continue
    return False


def withdraw_contacts(text):
    result = dict()
    infos = withdraw_contact(text)
    for (contact_type, contacts) in infos:
        if not result.get(contact_type.replace('1', '')):
            result[contact_type.replace('1', '')] = []
        result[contact_type.replace('1', '')].extend(contacts)
    return result


text = """
    my威信 三六九bas②斯Ⅰ ，口口依旧思午餐路扒一扒。 15768942321
    das36721ert 我的薇儿♥
"""
# text = 'dsadsdsaf sadqee dsad re dsdakkl  ds dsfdf ssa a sdas sasd12wq weixin'
print(replace_contact(text))
print(contain_contacts(text))
print(withdraw_contacts(text))
