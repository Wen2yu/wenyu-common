# -*- coding:utf-8 -*-

# Author : 'zhangjiawen'
# Date : 2019/11/18 0018 12:40

import calendar
import pymysql
import math
import re
import sys
import time
import traceback
from datetime import datetime, timedelta, date
from functools import wraps

from jy_conf.db.mysql import mysql_infos
from jy_crypt.jyvip_convert import convert

MONTHS = 12
HOURS = 23
MINUTES = 59
SECONDS = 59

DATE_FMT = '%Y-%m-%d'
DATE_FMT0 = '%y-%m-%d'
DATE_FMT_HMS = '%Y-%m-%d %H:%M:%S'
DATE_FMT_IMS = '%Y-%m-%d %I:%M:%S'
DATE_FMT0_HMS = '%y-%m-%d %H:%M:%S'
DATE_FMT0_IMS = '%y-%m-%d %I:%M:%S'
DATE_FMT_Ymd = '%Y%m%d'
DATE_FMT_Ym = '%Y%m'
DATE_FMT_ymd = '%y%m%d'
DATE_FMT_ym = '%y%m'
DATE_FMT_Ymd_HMS = '%Y%m%d %H:%M:%S'
DATE_FMT_Ymd_IMS = '%Y%m%d %I:%M:%S'
DATE_FMT_ymd_HMS = '%y%m%d %H:%M:%S'
DATE_FMT_ymd_IMS = '%y%m%d %I:%M:%S'
DAY_BEGIN = '00:00:00'
DAY_BEGIN12 = '00:00:00 am'
DAY_END = '23:59:59'
DAY_END12 = '23:59:59'

ZERO_DATETIME = datetime(1970, 1, 1)
DATE_20190101 = datetime(2019, 1, 1)
DATE_20210201 = datetime(2021, 2, 2)

date_str_switch = {
    DATE_FMT: lambda y, mo, d, h, mi, s: '%s-%s-%s' % format_numbers(y, mo, d),
    DATE_FMT_HMS: lambda y, mo, d, h, mi, s: '%s-%s-%s %s:%s:%s' % format_numbers(y, mo, d, h, mi, s),
    DATE_FMT_Ymd: lambda y, mo, d, h, mi, s: '%s%s%s' % format_numbers(y, mo, d),
    DATE_FMT_ymd: lambda y, mo, d, h, mi, s: '%s%s%s' % format_numbers(y % 100, mo, d),
    DATE_FMT_Ym: lambda y, mo, d, h, mi, s: '%s%s' % format_numbers(y, mo),
    DATE_FMT_ym: lambda y, mo, d, h, mi, s: '%s%s' % format_numbers(y % 100, mo),
    DATE_FMT_Ymd_HMS: lambda y, mo, d, h, mi, s: '%s%s%s %s:%s:%s' % format_numbers(y, mo, d, h, mi, s),
    '%Y-%m': lambda y, mo, d, h, mi, s: '%s-%s' % format_numbers(y, mo),
    '%Y.%m': lambda y, mo, d, h, mi, s: '%s.%s' % format_numbers(y, mo),
    '%Y.%m.%d': lambda y, mo, d, h, mi, s: '%s.%s.%s' % format_numbers(y, mo, d),
    '%Y年%m月': lambda y, mo, d, h, mi, s: '%s年%s月' % format_numbers(y, mo),
    '%y年%m月': lambda y, mo, d, h, mi, s: '%s年%s月' % format_numbers(y % 100, mo),
    '%y%m': lambda y, mo, d, h, mi, s: '%s%s' % format_numbers(y % 100, mo),
    '%y-%m': lambda y, mo, d, h, mi, s: '%s-%s' % format_numbers(y % 100, mo),
    '%y.%m': lambda y, mo, d, h, mi, s: '%s.%s' % format_numbers(y % 100, mo),
    '%y.%m.%d': lambda y, mo, d, h, mi, s: '%s.%s.%s' % format_numbers(y % 100, mo, d),
    '%Y年.%m月': lambda y, mo, d, h, mi, s: '%s年.%s月' % format_numbers(y, mo),
    '%Y年.%m月.%d日': lambda y, mo, d, h, mi, s: '%s年.%s月.%s日' % format_numbers(y, mo, d),
    '%y年.%m月': lambda y, mo, d, h, mi, s: '%s年.%s月' % format_numbers(y % 100, mo),
    '%y年.%m月.%d日': lambda y, mo, d, h, mi, s: '%s年.%s月.%s日' % format_numbers(y % 100, mo, d),
    '%Y年': lambda y, mo, d, h, mi, s: '%s年' % format_numbers(y),
    '%y年': lambda y, mo, d, h, mi, s: '%s年' % format_numbers(y % 100),
    '%Y': lambda y, mo, d, h, mi, s: '%s' % format_numbers(y),
    '%y': lambda y, mo, d, h, mi, s: '%s' % format_numbers(y % 100),
}


def format_numbers(*numbers: tuple, min_len=2, max_len=0, fill_num=0, fill_loc=1, sub_loc=1):
    """
    格式化时间元素数字
    :param numbers: 带格式化数字组
    :param min_len: 最小长度
    :param max_len: 最大长度
    :param fill_num: 不足最小长度时填充的数字
    :param fill_loc: 填充位置：1：左填充；-1：右填充
    :param sub_loc: 超出长度截取位置：1：左截取；-1：右截取
    :return:
    """
    result = []
    for number in numbers:
        ns = str(number)[:sub_loc * max_len] if max_len and sub_loc > 0 else str(number)[sub_loc * max_len:]
        result.append(
            str(fill_num) * (min_len - len(ns)) + ns if fill_loc > 0 else ns + str(fill_num) * (min_len - len(ns))
        )
    return tuple(result)


def from_timestamp(timestamp):
    """
    时间戳转datetime
    :param timestamp: 时间戳
    :return:
    """
    return datetime.fromtimestamp(timestamp)


def now(**kw):
    return datetime.now()


def now_timestamp():
    return now().timestamp()


def now_str(fmt=DATE_FMT_HMS):
    """
    获取当前时间字符串
    :param fmt: 默认DATE_FMT_HMS
    :return:
    """
    return time.strftime(fmt)


def cur_day_str(fmt=DATE_FMT):
    """
    获取当前日期字符串
    :param fmt: 默认DATE_FMT
    :return:
    """
    return time.strftime(fmt)


def check_day():
    def wrapper(func):

        @wraps(func)
        def inner(*args, **kwargs):
            if args and not args[0]:
                args = (now(), ) + args[1:]
            elif not args and kwargs.get('day') is None:
                kwargs['day'] = now()
            return func(*args, **kwargs)

        return inner
    return wrapper


@check_day()
def day_ts(day=None):
    """
    获取给定时间的时间戳
    :param day: 默认为当前时间
    :return:
    """
    return day.timestamp()


@check_day()
def day_begin(day=None, year=None, month=None, days=None, dealt=0, **kw):
    """
    获取给定日期开始时间
    :param day: 默认为当前时间
    :param year:
    :param month:
    :param days: 指定天
    :param dealt: 加减天数
    :return:
    """
    return datetime(year or day.year, month or day.month, days or day.day) + timedelta(dealt)


@check_day()
def day_end(day=None, year=None, month=None, days=None, dealt=0, **kw):
    """
    获取给定日期结束时间
    :param day: 默认为当前时间
    :param year:
    :param month:
    :param days: 指定天
    :param dealt: 加减天数
    :return:
    """
    return datetime(year or day.year, month or day.month, days or day.day, hour=23, minute=59, second=59) + timedelta(dealt)


@check_day()
def yesterday_begin(day=None, year=None, month=None, days=None, dealt=0, **kw):
    """
    获取给定日期昨天开始时间
    :param day: 默认为当前时间
    :param year:
    :param month:
    :param days: 指定天
    :param dealt: 加减天数
    :return:
    """
    return day_begin(day, year, month, days, dealt=dealt) - timedelta(days=1)


@check_day()
def yesterday_end(day=None, year=None, month=None, days=None, dealt=0, **kw):
    """
    获取给定日期昨天结束时间
    :param day: 默认为当前时间
    :param year:
    :param month:
    :param days: 指定天
    :param dealt: 加减天数
    :return:
    """
    return day_end(day, year, month, days, dealt=dealt) - timedelta(days=1)


@check_day()
def month_begin(day=None, year=None, month=None, months=0, **kw):
    """
    获取给定日期月初时间
    :param day: 默认为当前时间
    :param year:
    :param month:
    :param months:
    :return:
    """
    return add_months(datetime(year or day.year, month or day.month, 1), months)


@check_day()
def month_end(day=None, year=None, month=None, months=0, **kw):
    """
    获取给定日期月末时间
    :param day: 默认为当前时间
    :param year:
    :param month:
    :param months:
    :return:
    """
    if year:
        day = add_months(datetime(year, month or day.month, 1), months)
    else:
        day = add_months(month_begin(day), months)
    day = day + timedelta(month_days(day)[1] - 1)
    return datetime(day.year, day.month, day.day, hour=23, minute=59, second=59)


@check_day()
def last_month_begin(day=None, year=None, month=None, **kw):
    """
    获取给定日期上个月月初时间
    :param day: 默认为当前时间
    :param year:
    :param month:
    :return:
    """
    return month_begin(add_months(day, -1), year, month)


@check_day()
def last_month_end(day=None, year=None, month=None, **kw):
    """
    获取给定日期上个月月末时间
    :param day: 默认为当前时间
    :param year:
    :param month:
    :return:
    """
    return month_end(add_months(day, -1))


@check_day()
def year_begin(day=None, year=None, **kw):
    """
    获取给定日期年初时间
    :param day: 默认为当前时间
    :param year:
    :return:
    """
    return datetime(year or day.year, 1, 1)


@check_day()
def year_end(day=None, year=None, **kw):
    """
    获取给定日期年末时间
    :param day: 默认为当前时间
    :param year:
    :return:
    """
    day = datetime(year or day.year, 1, 1)
    day = month_end(add_months(day, 11))
    return datetime(day.year, day.month, day.day, hour=23, minute=59, second=59)


def cur_day_begin_str():
    """当前日期开始时间字符串"""
    return '%s %s' % (cur_day_str(), DAY_BEGIN)


def cur_day_end_str():
    """当前日期结束时间字符串"""
    return '%s %s' % (cur_day_str(), DAY_END)


def day_str(day: datetime, fmt=DATE_FMT_HMS):
    """
    获取给定时间的字符串
    :param day:
    :param fmt: 时间格式，默认DATE_FMT_HMS
    :return:
    """
    return date_str_switch[fmt](
        day.year, day.month, day.day, getattr(day, 'hour', 0), getattr(day, 'minute', 0), getattr(day, 'second', 0)
    ) if date_str_switch.get(fmt) else day.strftime(fmt)



@check_day()
def quarter(day: datetime):
    """
    返回给定日期所在季度yyyyQi，eg:2021Q4
    :param day:
    :return:
    """
    return f'{day.year}Q{math.ceil(day.month/3)}'


def cur_day_begin():
    """当前日期开始时间"""
    n = now()
    return n - timedelta(hours=n.hour, minutes=n.minute, seconds=n.second, microseconds=n.microsecond)


def cur_day_end():
    """当前日期结束时间"""
    return cur_day_begin() + timedelta(hours=HOURS, minutes=MINUTES, seconds=SECONDS)


def yesterday_str(fmt=DATE_FMT_HMS):
    """
    昨天开始时间字符串
    :param fmt: 格式，默认DATE_FMT_HMS
    :return:
    """
    return day_str(yesterday_begin(), fmt=fmt)


@check_day()
def add_days(day, days):
    """
    加days天
    :param day:
    :param days:
    :return:
    """
    return day + timedelta(days)


def add_months(day, months, p_day=0):
    """
    加monthss月
    :param day:
    :param months:
    :param p_day:
    :return:
    """
    if months == 0:
        return datetime(day.year, day.month, p_day, day.hour, day.minute, day.second) if p_day else day
    month = (day.month + months) % MONTHS
    year = day.year + (day.month + months - 1) // MONTHS
    month = month if month else MONTHS
    return datetime(
        year, month, p_day if p_day else min(day.day,  calendar.monthrange(year, month)[1]),
        day.hour, day.minute, day.second
    )


def month_first(day, days=1):
    """
    给定时间的月初
    :param day:
    :param days: 日期偏移量
    :return:
    """
    return datetime(day.year, day.month, days)


def month_first_str(day, fmt=DATE_FMT, hms=False):
    """
    给定时间的月初字符串
    :param day:
    :param fmt:
    :param hms:
    :return:
    """
    return '%s%s' % (date_str_switch[fmt](day.year, day.month, 1),
                     ' %s' % DAY_BEGIN if hms else '')


def month_last_str(day, fmt=DATE_FMT, hms=False):
    """
    给定时间的月末字符串
    :param day:
    :param fmt:
    :param hms:
    :return:
    """
    return '%s%s' % (date_str_switch[fmt](day.year, day.month, month_days(day)[1]),
                     ' %s' % DAY_END if hms else '')


def month_last(day):
    """
    给定时间的月末
    :param day:
    :return:
    """
    return month_first(day) + timedelta(month_days(day)[1] - 1, hours=23, minutes=59, seconds=59)


def month_days(day):
    """
    获取给定日期当月的天数
    :param day:
    :return: (当月第一天的星期序号[0:6], 当月总天数)
    """
    return calendar.monthrange(day.year, day.month)


month_word3 = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
dt_attr_value_switch = {
    'y': lambda v: int(now().year/100 - int(int(v) > now().year % 100)) * 100 + int(v) if len(v) < 3 else int(v),
    'm': lambda v: month_word3.index(v.lower()) if len(v) == 3 else int(v),
    'd': lambda v: int(v),
    'H': lambda v: int(v),
    'h': lambda v: int(v),
    'M': lambda v: int(v),
    's': lambda v: int(v),
    'A': lambda v: int(v in ['PM', 'P', '下午', '下'])
}
datetime_patterns = [
    (re.compile(r'(\d{2,4})[-/年](\d{1,2})[-/月](\d{1,2})[日]?\s+(\d{1,2})[:时](\d{1,2})[:分](\d{1,2})[秒]?\s+([A/P]M?)'), 'ymdhMsA'),
    (re.compile(r'(\d{2,4})[-/年](\d{1,2})[-/月](\d{1,2})[日]?\s+(\d{1,2})[:时](\d{1,2})[:分](\d{1,2})[秒]?'), 'ymdHMs'),
    (re.compile(r'(\d{2,4})[-/年](\d{1,2})[-/月](\d{1,2})[日]?'), 'ymd'),
    (re.compile(r'(\d{2,4})[-/\\.年](\d{1,2})[月]?'), 'ym'),
    (re.compile(r'(\d{1,2})[-/月](\d{1,2})[日]?'), 'md'),
    (re.compile(r'(\d{2,4})[-/]([a-zA-Z]{3})[-/](\d{1,2})'), 'ymd'),
    (re.compile(r'(\d{1,2})[-/]([a-zA-Z]{3})[-/](\d{4})'), 'dmy'),
    (re.compile(r'(\d{2,4})[-/]([a-zA-Z]{3})'), 'ym'),
    (re.compile(r'([a-zA-Z]{3})[-/](\d{4})'), 'my'),
    (re.compile(r'(\d{1,2})[:时](\d{1,2})[:分](\d{1,2})[秒]?\s+([A/P]M?)'), 'hMsA'),
    (re.compile(r'(\d{1,2})[:时](\d{1,2})[:分]\s+([A/P]M?)'), 'hMsA'),
    (re.compile(r'([上下]午)\s?(\d{1,2})[:时](\d{1,2})[:分](\d{1,2})[秒]?'), 'AhMs'),
    (re.compile(r'([上下]午)\s?(\d{1,2})[:时](\d{1,2})[分]?'), 'AhM'),
    (re.compile(r'(\d{1,2})[:时](\d{1,2})[:分](\d{1,2})[秒]?'), 'HMs'),
    (re.compile(r'(\d{1,2})[:时](\d{1,2})[分]?'), 'HM'),
    (re.compile(r'Month (\d{1,2})\((\d{2,4})\)'), 'my'),
]


def str_to_date(st, fmt=DATE_FMT):
    """
    字符串转日期
    :param st: 字符串
    :param fmt: 格式，默认：DATE_FMT
    :return:
    """
    if not st or len(st) < 4:
        return st
    if not fmt:
        for (pat, s) in datetime_patterns:
            match = pat.fullmatch(st)
            if match:
                groups = match.groups()
                avs = {s[i]: dt_attr_value_switch[s[i]](groups[i]) for i in range(len(s))}
                if avs.get('A'):
                    avs['H'] = avs.get('h') + avs['A'] * 12
                return datetime(
                    year=avs.get('y', 1970), month=avs.get('m', 1), day=avs.get('d', 1),
                    hour=avs.get('H', 0), minute=avs.get('M', 0), second=avs.get('s', 0)
                )
    return datetime.strptime(st, fmt)


def is_number(val):
    """
    判断给定的值是否为数字
    :param val:
    :return:
    """
    if type(val) in [int, float]:
        return True
    if type(val) == str:
        try:
            float(val)
            return True
        except ValueError:
            return False
    return False


def convert_val_to_datetime(val, fmt=DATE_FMT_HMS):
    """
    值转时间
    :param val: 值
    :param fmt: 格式，默认：DATE_FMT_HMS
    :return:
    """
    if not fmt:
        fmt = DATE_FMT_HMS
    if type(val) == datetime:
        return val
    elif type(val) == int or is_number(val):
        return datetime.fromtimestamp(val) if val >= 86400 else datetime(1970, 1, 1)
    elif type(val) == date:
        return datetime(val.year, val.month, val.day)
    elif type(val) == str:
        return datetime.strptime(val, fmt)
    else:
        return None


@check_day()
def the_n_workday(day=None, n=0, months=0):
    result = day
    if months:
        day = add_months(day, months)
    if n:
        conn, csr = None, None
        try:
            conf = mysql_infos['jydata_yj']
            conn = pymysql.connect(host=conf['host'], user=conf['user'], passwd=convert(conf['passwd'], 1), db=conf['db'])
            csr = conn.cursor()
            csr.execute(f'select * from work_calendar where year = {day.year} and month = {day.month} and type > 0 and rownum <= {n}')
            result = datetime(*csr.fetchall()[-1][1:4])
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        finally:
            if csr:
                csr.close()
            if conn:
                conn.close()
    return result
