# -*- coding:utf-8 -*-

# Name: mysql
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/6/17 12:34


USER = 'jifei_special'
PASSWD = '1n2d1i273k353h2d29371i1o3j3m3h32'
JYDATA_USER = 'hadoop'
JYDARA_PASSWD = '3132331h1i1j11'
TEST_USER = 'test'
TEST_PASSWD = '1k1i1i1h1h1i'

mysql_infos: {str, dict} = {
    'jydata_etl': {
        'host': 'cdh-14mysql.master.sql.baihe',
        'user': JYDATA_USER,
        'passwd': JYDARA_PASSWD,
        'db': 'etl',
    },
    'jydata_kpi': {
        'host': 'cdh-14mysql.master.sql.baihe',
        'user': JYDATA_USER,
        'passwd': JYDARA_PASSWD,
        'db': 'kpi',
    },
    'jydata_bi': {
        'host': 'cdh-14mysql.master.sql.baihe',
        'user': JYDATA_USER,
        'passwd': JYDARA_PASSWD,
        'db': 'bi',
    },
    'jydata_ror': {
        'host': '10.200.100.14',
        'user': JYDATA_USER,
        'passwd': JYDARA_PASSWD,
        'db': 'ror',
    },
    'crm': {
        # 'url': f'{USER}:{PASSWD}@crm-script.slave.sql.jiayuan/crm',  # url和下面的信息二选一即可
        'host': 'crm-script.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'crm',
    },
    'saas': {
        'host': 'crm-jifei.master.sql.jiayuan',
        'user': 'crm_jifei',
        'passwd': '3f2u3j323m263927201n3b2q3a232j2l1h2o',
        'db': 'saas',
    },
    'mid_dc2bi': {
        'host': 'adb.master.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'mid_dc2bi',
    },
    'wx_jybh': {
        'host': 'adb.master.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'wx_jybh',
    },
    'bh_vipcrm': {
        'host': 'bhvipcrm-bak.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'bh_vipcrm',
    },
    'jybhcrm': {
        'host': 'jybhcrm-jiaoben.slave.sql.baihe',
        'user': USER,
        'passwd': PASSWD,
        'db': 'jybhcrm',
    },
    'baihe_app': {
        'host': 'app.slave.sql.baihe',
        'user': USER,
        'passwd': PASSWD,
        'db': 'app',
    },
    'baihe_spms': {
        'host': 'payment56.slave.sql.baihe',
        'user': USER,
        'passwd': PASSWD,
        'db': 'baihe_spms',
    },
    'baihe_userCloud_credit': {
        'host': 'baihe-userCloud-userAccount.slave.sql.baihe',
        'user': USER,
        'passwd': PASSWD,
        'db': 'baihe_userCloud_credit',
    },
    'jy_oa': {
        'host': 'oa.master.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'jy_oa',
    },
    'jy_user_master': {
        'host': 'user.master.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'love21cn',
    },
    'jy_user_slave1': {
        'host': 'user-01.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'love21cn',
    },
    'jy_user_slave2': {
        'host': 'user-02.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'love21cn',
    },
    'jy_user_slave3': {
        'host': 'user-03.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'love21cn',
    },
    'background': {
        'host': 'background.master.sql.jiayuan',
        'user': 'sg',
        'passwd': '3c353k3k353i',
        'db': 'love21cn',
    },
    'love21cn': {
        'host': 'user-04.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'love21cn',
    },
    'h5_master': {
        'host': 'party.master.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'love21cn',
    },
    'h5': {
        'host': 'party-02.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'love21cn',
    },
    'subject': {
        'host': 'subject.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'subject',
    },
    'giftsystem': {
        'host': 'giftsystem-01.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'giftsystem',
    },
    'newsearch': {
        'host': 'newsearch-01.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'newsearch',
    },
    'moments': {
        'host': 'wapfriend-01.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'moments',
    },
    'friend': {
        'host': 'friend-01.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'love21cn',
    },
    'matchlist': {
        'host': 'static-01.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'matchlist',
    },
    'bhvip_avatar': {
        'host': 'vip-avatar-business.slave.sql.baihe',
        'user': USER,
        'passwd': PASSWD,
        'db': 'avatar',
    },
    'datecrm': {
        'host': 'datecrm.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'datecrm',
    },
    'bhcrm': {
        'host': 'bhdatecrm.slave.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'bhcrm',
    },
    'check_log': {
        'host': 'shenhelog.master.sql.jiayuan',
        'user': USER,
        'passwd': PASSWD,
        'db': 'check_log',
    },
    'localhost': {
        'host': '127.0.0.1',
        'user': TEST_USER,
        'passwd': TEST_PASSWD,
        'db': 'mysql',
    },
    'crawl': {
        'host': '127.0.0.1',
        'user': TEST_USER,
        'passwd': TEST_PASSWD,
        'db': 'crawl',
    },
}


def get_mysql_conf(key: str) -> dict:
    return mysql_infos.get(key, dict())

