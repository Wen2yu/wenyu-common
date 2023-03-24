# -*- coding: utf-8 -*-

# Author : 'zhangjiawen'
import calendar
from datetime import datetime
from calendar import monthcalendar


def month_work_days(day: datetime, weekends=(5, 6), holidays: list = None, workdays: list = None):
    """
    获取给定日期所在月份的工作日序号list和休息日序号list
    :param day: 给定日期
    :param weekends: 固定周休序号，默认为(5, 6)， 对应为(周六,周日)
    :param holidays: 自定义节假日(list)
    :param workdays: 自定义工作日(list)，调班
    :return:(工作日序号list,休息日序号list)
    """
    year, month = day.year, day.month
    work_days, rest_days = [], []
    week_day_lists = monthcalendar(year, month)
    for week_days in week_day_lists:
        weekend_days = []
        for wd in weekends:
            weekend_days.append(week_days[wd])
        for d in week_days:
            if d == 0:
                continue
            if holidays and d in holidays or (
                    d in weekend_days and (not workdays or workdays and d not in workdays)):
                rest_days.append(d)
            else:
                work_days.append(d)
    return work_days, rest_days


def month_nrd_work_day(
        day: datetime, nrd: int, weekends=(5, 6), holidays: list = None, workdays: list = None, is_full=False
) -> datetime:
    """
    获取给定日期所在月份第n工作日
    :param day: 给定日期
    :param nrd: 第n
    :param weekends: 固定周休序号，默认为(5, 6)， 对应为(周六,周日)
    :param holidays: 自定义节假日(list)
    :param workdays:自定义工作日(list)，调班
    :param is_full: 自定义工作日是否是全部工作日
    :return:
    """
    year, month = day.year, day.month
    if is_full:
        return datetime(year, month, workdays[nrd]) if workdays and len(workdays) >= nrd else None
    else:
        week_day_lists = monthcalendar(year, month)
        index, has_found, found_day = 0, False, 0
        for week_days in week_day_lists:
            weekend_days = []
            for wd in weekends:
                weekend_days.append(week_days[wd])
            for d in week_days:
                if d == 0:
                    continue
                if holidays and d in holidays or (
                        d in weekend_days and (not workdays or workdays and d not in workdays)):
                    continue
                index += 1
                if index == nrd:
                    has_found, found_day = True, d
                    break
            if has_found:
                break
        return datetime(year, month, found_day) if found_day else None
