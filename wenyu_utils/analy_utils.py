# -*- coding:utf-8 -*-

# Name: analy_utils
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2022/2/7 15:15

import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit


x = np.array([
    155495385,122629887,172266268,163787673,166796790,156287248,152740701,154179073,141749519,152682264,189397743,
    198668842,182379685,176460160,201312559,185471358,186805253,169932564,179212150,178238914,166992721,168779526,
    155951243,151107301,127046801,136815102,153173290,130543852,139303632,137521087,139435906,140330343,149040536,
    156952536,140634446,144366983
])


y = np.array([
    26372146,26637192,33520999,29758371,31304698,28793326,28446490,29194308,25171179,27109914,27159887,25844627,
    25669322,29811642,27892539,26374355,27031109,23815527,24269392,23817093,22440891,23109359,21331927,19952025,
    20997441,23656109,21388853,20432200,22535863,20588527,20888275,20437449,18438356,18269426,16523795,16397209
])


def yvals(x: np.ndarray, y: np.ndarray, level):
    return np.polyval(np.polyfit(x, y, level), x)


def show(x: np.ndarray, y: np.ndarray, level):
    plot1 = plt.plot(x, y, 's', label='original values')
    plot2 = plt.plot(x, yvals(x, y, level), 'r', label='polyfit values')
    plt.xlabel('x')
    plt.ylabel('y')
    plt.legend(loc=4)  # 指定legend的位置右下角
    plt.title('polyfitting')
    plt.show()


def curve_fit_yvals(x, y, func):
    popt, pcov = curve_fit(func, x, y)
    return



