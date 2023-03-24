# 更名表列名
rname_col_names = [
    'tdate', 'orderno', 'pro_id', 'type', 'new_name', 'old_name', 'nc_shop_name', 'author', 'crank', 'brand',
    'settle_brand', 'sup_id', 'shop_id', 'shop_name', 'nc_shop_name'
]


def make_cv_rname(rordb, **kw):
    """

    :param rordb:
    :param kw:
    :return:
    """
    v_sql = '''
  select tdate, orderno, '0' as pro_id, 0 as type, 
         uname as new_name, old_uname as  old_name, 
         null as author, is_cooperation as crank, 
         brand, account_brand as settle_brand, ca_id as sup_id, shop_id, 
         sname as shop_name
    from cv_order_difference_tmp
   where tdate = '#tdate'
     and uname <> old_uname
  '''
    rname_datas = parse_data(*rordb.select(v_sql, tdate=kw.get('tdate')))
    rordb.save("cv_rname", rname_datas, rname_col_names)
