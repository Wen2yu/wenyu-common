# -*- coding:utf-8 -*-

# Name: jyol_crypt
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/9/17 14:58

from jy_utils.str_utils import zip_str, sorted_str


base = {
    1: 'a', 2: 'b', 3: 'c', 4: 'd', 5: 'e', 6: 'f', 7: 'g', 8: 'h', 9: 'i', 10: 'j', 11: 'k', 12: 'l', 13: 'm', 14: 'n',
    15: 'o', 16: 'p', 17: 'q', 18: 'r', 19: 's', 20: 't', 21: 'u', 22: 'v', 23: 'w', 24: 'y', 0: 'z'
}
tik = 11  # 偏移量


def encrypt_idc(idc: str):
    """
    加密线上身份证号
    :type idc: str  待加密的身份证号
    """
    return ''.join([
        idc[i] if idc[i].upper() == 'X' else (
            base.get(int(idc[i])) if i == tik else base.get(int(idc[i]) + int(int(idc[int(idc[tik])])))
        ) for i in range(0, len(idc))
    ])


def decrypt_idc(en_idc: str):
    """
    解密线上身份证号
    :param en_idc: 待解密的身份证号密文
    :return:
    """
    _base = {v: k for k, v in base.items()}
    return '' if len(en_idc) <= tik else (''.join(
        [
            en_idc[i] if en_idc[i].upper() == 'X' else (
                str(_base.get(en_idc[i])) if i == tik else str(int(
                    _base.get(en_idc[i]) - _base.get(en_idc[int(_base[en_idc[tik]])]) / 2
                ))
            ) for i in range(0, len(en_idc))
        ]
    ) if _base.get(en_idc[tik]) is not None else '')


def _utf8_2_unicode(utf8_str):
    unicode = (ord(utf8_str[0:1]) & 0x1F) << 12
    unicode |= (ord(utf8_str[1:2]) & 0x3F) << 6
    unicode |= (ord(utf8_str[2:3]) & 0x3F)
    return hex(unicode)[2:]


def _unicode_2_utf8(unicode_str):
    code = str(int(unicode_str.upper(), 16))
    bs = chr(int(str(bin(0xe0 | (int(code) >> 12))), 2)).encode('latin1') \
         + chr(int(str(bin(0x80 | ((int(code) >> 6) & 0x3f))), 2)).encode('latin1') \
         + chr(int(str(bin(0x80 | (int(code) & 0x3f))), 2)).encode('latin1')
    print(bs)
    return (
            chr(int(str(bin(0xe0 | (int(code) >> 12))), 2)).encode('latin1') +
            chr(int(str(bin(0x80 | ((int(code) >> 6) & 0x3f))), 2)).encode('latin1') +
            chr(int(str(bin(0x80 | (int(code) & 0x3f))), 2)).encode('latin1')
    ).decode()


def crypt_name(name: str):
    """
    加解密姓名
    :param name: 待加(解)密的姓名（密文）
    :return:
    """
    s = zip_str(name)
    return ''.join([
        _unicode_2_utf8(
            sorted_str(_utf8_2_unicode(s[k].encode()), idxes='0132')
        ) if '\u4e00' <= s.get(k) <= '\u9fff' else s[k] for k in s
    ])


print(crypt_name('张佳文'))

