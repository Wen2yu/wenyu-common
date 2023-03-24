# -*- coding:utf-8 -*-

# Name: panda_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2021/10/28 10:49

import os
import pandas as pd


bath_path = 'D:\\workSpace\\jiaoben\\Data\\投诉退费体系\\21年支付宝'


def excel_files(path):
    """
    获取ExcelFile生成器
    :param path:
    :return:
    """
    return ((f, pd.ExcelFile(f'{path}\{f}')) for f in os.listdir(path) if f.endswith(('.xlsx', '.xls')))


def read_excels_data(excels):
    """

    :param excels:
    :return:
    """
    return {file_name: {
        sheet_name: excel.parse(sheet_name) for sheet_name in excel.sheet_names
    } for file_name, excel in excels}


def write2csv(df: pd.DataFrame, filename: str, header=True, index=False):
    df.to_csv(filename, header=False, index=False)


for filename, d in read_excels_data(excel_files(bath_path)).items():
    for sheet_name, sheet_df in d.items():
        write2csv(sheet_df, f"{bath_path}\{'.'.join(filename.split('.')[:-1])}-{sheet_name}.csv")
