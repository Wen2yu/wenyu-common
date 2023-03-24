# -*- coding:utf-8 -*-

# Name: script
# Product_name: PyCharm
# Description:
# Author : 'zhangjiawen'
# Date : 2022/9/15 14:13

import sys
from openpyxl import Workbook
from openpyxl.drawing import image
from PIL import Image, ImageDraw, ImageFont

from jy_db import *
from jy_utils.http_utils import *
from jy_utils.sys_utils import *


sql_dict = {
    '邮票': {
        'iOS': """select sum(b.valid_stamp * b.stamp_dj), b.platform_id
            from valid_order_sj2 a, valid_consume_sj2 b
            where a.id = b.valid_order_id
                and a.pay_type_id in (60, 76)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '盈币': """select sum(b.valid_stamp * b.stamp_dj), b.platform_id
            from valid_order_sj2 a, valid_consume_sj2 b
            where a.id = b.valid_order_id
                and a.pay_type_id in (37)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '易联手机支付': """select sum(b.valid_stamp * b.stamp_dj), b.platform_id
            from valid_order_sj2 a, valid_consume_sj2 b
            where a.id = b.valid_order_id
                and a.pay_type_id in (92)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '支付宝': """select sum(b.valid_stamp * b.stamp_dj), b.platform_id
            from valid_order_sj2 a, valid_consume_sj2 b
            where a.id = b.valid_order_id
                and a.pay_type_id in (17)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '支付宝超级扫码': """select sum(b.valid_stamp * b.stamp_dj), b.platform_id
            from valid_order_sj2 a, valid_consume_sj2 b
            where a.id = b.valid_order_id
                and a.pay_type_id in (148)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '财付通': """select sum(b.valid_stamp * b.stamp_dj), b.platform_id
            from valid_order_sj2 a, valid_consume_sj2 b
            where a.id = b.valid_order_id
                and a.pay_type_id in (31)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '微信wap支付': """select sum(b.valid_stamp * b.stamp_dj), b.platform_id
            from valid_order_sj2 a, valid_consume_sj2 b
            where a.id = b.valid_order_id
                and a.pay_type_id in (113)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '微信SDK代扣': """select sum(b.valid_stamp * b.stamp_dj), b.platform_id
            from valid_order_sj2 a, valid_consume_sj2 b
            where a.id = b.valid_order_id
                and a.pay_type_id in (126)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '微信公众号_无线': """select sum(b.valid_stamp * b.stamp_dj), b.platform_id
            from valid_order_sj2 a, valid_consume_sj2 b
            where a.id = b.valid_order_id
                and a.pay_type_id in (123)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
             group by b.platform_id
             order by b.platform_id""",
        '微信扫码支付': """SELECT SUM(B.VALID_STAMP * B.STAMP_DJ), B.PLATFORM_ID
            FROM VALID_ORDER_SJ2 A, VALID_CONSUME_SJ2 B
            WHERE A.ID = B.VALID_ORDER_ID
                AND A.PAY_TYPE_ID IN (145)
                AND B.BUYTIME >= date'#mth'
                AND B.BUYTIME < date_add(date'#mth', interval 1 month)
            GROUP BY B.PLATFORM_ID
            ORDER BY B.PLATFORM_ID""",
        '微信APP（新版）': """select sum(b.valid_stamp * b.stamp_dj), b.platform_id
            from valid_order_sj2 a, valid_consume_sj2 b
            where a.id = b.valid_order_id
                and a.pay_type_id in (122)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
    },
    "宝": {
        'iOS': """select sum(b.valid_bao * b.bao_dj), b.platform_id
            from valid_bao_sj2 a, valid_bao_consume_sj2 b
            where a.id = b.valid_bao_id
                and a.paytype_id in (60, 76)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '盈币': """select sum(b.valid_bao * b.bao_dj), b.platform_id
            from valid_bao_sj2 a, valid_bao_consume_sj2 b
            where a.id = b.valid_bao_id
                and a.paytype_id in (37)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '易联手机支付': """select sum(b.valid_bao * b.bao_dj), b.platform_id
            from valid_bao_sj2 a, valid_bao_consume_sj2 b
            where a.id = b.valid_bao_id
                and a.paytype_id in (92)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '支付宝': """select sum(b.valid_bao * b.bao_dj), b.platform_id
            from valid_bao_sj2 a, valid_bao_consume_sj2 b
            where a.id = b.valid_bao_id
                and a.paytype_id in (17)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '支付宝超级扫码': """select sum(b.valid_bao * b.bao_dj), b.platform_id
            from valid_bao_sj2 a, valid_bao_consume_sj2 b
            where a.id = b.valid_bao_id
                and a.paytype_id in (148)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '财付通': """select sum(b.valid_bao * b.bao_dj), b.platform_id
            from valid_bao_sj2 a, valid_bao_consume_sj2 b
            where a.id = b.valid_bao_id
                and a.paytype_id in (31)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '微信wap支付': """select sum(b.valid_bao * b.bao_dj), b.platform_id
            from valid_bao_sj2 a, valid_bao_consume_sj2 b
            where a.id = b.valid_bao_id
                and a.paytype_id in (113)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '微信SDK代扣': """select sum(b.valid_bao * b.bao_dj), b.platform_id
            from valid_bao_sj2 a, valid_bao_consume_sj2 b
            where a.id = b.valid_bao_id
                and a.paytype_id in (126)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '微信公众号_无线': """select sum(b.valid_bao * b.bao_dj), b.platform_id
            from valid_bao_sj2 a, valid_bao_consume_sj2 b
            where a.id = b.valid_bao_id
                and a.paytype_id in (123)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '微信扫码支付': """select sum(b.valid_bao * b.bao_dj), b.platform_id
            from valid_bao_sj2 a, valid_bao_consume_sj2 b
            where a.id = b.valid_bao_id
                and a.paytype_id in (145)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
        '微信APP（新版）': """select sum(b.valid_bao * b.bao_dj), b.platform_id
            from valid_bao_sj2 a, valid_bao_consume_sj2 b
            where a.id = b.valid_bao_id
                and a.paytype_id in (122)
                and b.buytime >= date'#mth'
                and b.buytime < date_add(date'#mth', interval 1 month)
            group by b.platform_id
            order by b.platform_id""",
    }
}


def gen_text__list(sqls, **kw):
    """
    生成文本列表
    :return:
    """
    text__list = []
    ror = get_db('Mysql', 'jydata_ror', auto_close=False)
    empty_line = ' ' * 20
    try:
        for k in sqls:
            text__list.extend([empty_line, f"--------{k.encode('utf-8')}--------", empty_line])
            text__list.extend(gen_sql_with_params(sqls[k], **kw).split('\n'))
            text__list.append(empty_line)
            data, cols = ror.select(sqls[k], include_col_names=1, **kw)
            cols = [c[0] for c in cols]
            split = ['-' * len(c) for c in cols]
            data = [[(
                    ' ' * (len(cols[i]) - len(str(d[i].quantize(Decimal('0.01')) if type(d[i]) == Decimal else d[i])))
                    + str(d[i].quantize(Decimal('0.01')) if type(d[i]) == Decimal else d[i])
            ) for i in range(len(cols))] for d in data]
            text__list.extend(['  '.join([str(c) for c in d]) for d in ([cols, split] + data)])
    except Exception as e:
        text__list.extend(str(e).split('\n'))
    finally:
        ror.close()
    return text__list


def text2image(lines, out_path):
    """
    文字转图片
    :param content:
    :param out_path:
    :return:
    """
    font_size = 16
    width = font_size * 0.75 * max([len(line) for line in lines])
    height = font_size * len(lines) + 5
    image = Image.new("RGB", (width, height), (0, 0, 0))
    draw = ImageDraw.Draw(image)
    font = ImageFont.truetype('/u01/lab/downloads/SimHei.ttf', font_size)
    draw.text((0, 0), '\n'.join(lines), font=font, fill='#ffffff')
    image.save(out_path)
    return out_path


@jy_mail()
def gen_script_excel(mth, **kw):
    excel_path = '/u01/work/excel/form/财务/form_result/'
    for k, sqls in sql_dict.items():
        lines = gen_text__list(sqls, mth=mth)
        text2image(lines, f'{k}_{mth}.jpg')
    book = Workbook()
    for k, sqls in sql_dict.items():
        sheet = book.create_sheet(k)
        sheet.add_image(image.Image(f'{k}_{mth}.jpg'))
    k_w = kw.get('k_w', dict())
    k_w['subject'] = f'{mth[:4]}年{mth[5:7]}月脚本'
    file_path = f"{excel_path}{k_w['subject']}"
    book.save(file_path)
    k_w['files'] = {
        f"{k_w['subject']}.xlsx": file_path
    }
    kw['k_w'] = k_w
    return k_w['subject']


if __name__ == '__main__':
    args, kw = sys_args(*sys.argv[1:])
    gen_script_excel(*args, **kw)
