# -*- coding:utf-8 -*-

# Name: json_encoder
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2019-12-31 17:01

import json
from datetime import datetime, date
from decimal import Decimal


class DataEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        elif isinstance(obj,Decimal):
            return float(obj)
        else:
            return json.JSONEncoder.default(self, obj)
