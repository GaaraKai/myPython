import pandas as pd
import time
import os
import sys


def get_prod_id(row):
    a = row['支付产品编号']
    if pd.isnull(a):
        pass
    else:
        return a[0:8]

def get_prod_name(row):
    x = row['支付产品编号']
    if pd.isnull(x):
        pass
    else:
        return x[9:-1]


def get_mer_id(row):
    x = row['主商户号']
    if pd.isnull(x):
        pass
    else:
        return x[0:x.index('(')]

def get_mer_name(row):
    x = row['主商户号']
    if pd.isnull(x):
        pass
    else:
        return x[x.index('(') + 1:-1]


def get_trx_detail(parm_reader):
    rtn_df = pd.DataFrame({})
    prod_id = pd.Series()
    prod_name = pd.Series()
    mer_id = pd.Series()
    mer_name = pd.Series()
    for chunk in parm_reader:
        x = chunk[['机构号'
            , '机构请求流水'
            , '内部订单号'
            , '商户订单号'
            , '商户订单日期'
            , '下单IP'
            , '用户支付IP'
            , '同盾设备指纹'
            , '自研设备指纹'
            , '证件/机构代号'
            , '扣款(元)'
            , '支付日期'
            , '平台日期'
            , '平台时间'
            , '卡类型'
            , '交易状态'
            , '手机号'
            , '收款方卡号'
            , '收款方姓名/公司名称'
            , '发卡行'
            , '银行通道编号'
            , '银行返回流水'
            , '支付网关去银行流水'
            , '卡号（脱敏后卡号前六后四）'
            , '持卡人'
            , '返回码'
            , '下单IP归属地'
            , '用户支付IP归属地'
            , '预留手机号归属地']]
        rtn_df = rtn_df.append(x)

        # NEW FUNCTION
        prod_id_list = chunk.apply(get_prod_id, axis=1)
        prod_name_list = chunk.apply(get_prod_name, axis=1)
        mer_id_list = chunk.apply(get_mer_id, axis=1)
        mer_name_list = chunk.apply(get_mer_name, axis=1)
        # print('prod_id_list \n ',prod_id_list)
        prod_id = prod_id.append(prod_id_list)
        prod_name = prod_name.append(prod_name_list)
        mer_id = mer_id.append(mer_id_list)
        mer_name = mer_name.append(mer_name_list)
        rtn_df["prod_id"] = prod_id
        rtn_df["prod_name"] = prod_name
        rtn_df["mer_id"] = mer_id
        rtn_df["mer_name"] = mer_name

    rtn_df.rename(columns={'机构号': 'inst_id'
        , '机构请求流水': 'inst_trace'
        , '内部订单号': 'inner_trade_id'
        , '商户订单号': 'order_id'
        , '商户订单日期': 'order_date'
        , '下单IP': 'order_ip'
        , '用户支付IP': 'pay_ip'
        , '同盾设备指纹': 'td_device'
        , '自研设备指纹': 'zy_device'
        , '证件/机构代号': 'id_no'
        , '扣款(元)': 'trx_amount'
        , '支付日期': 'pay_date'
        , '平台日期': 'plat_date'
        , '平台时间': 'plat_time'
        , '卡类型': 'card_type'
        , '交易状态': 'trx_status'
        , '手机号': 'mobile_no'
        , '收款方卡号': 'recv_card_no'
        , '收款方姓名/公司名称': 'recv_name'
        , '发卡行': 'issue_bank'
        , '银行通道编号': 'bank_channel'
        , '银行返回流水': 'bank_rtn_trace'
        , '支付网关去银行流水': 'gateway_trace'
        , '卡号（脱敏后卡号前六后四）': 'card_no'
        , '持卡人': 'card_holder'
        , '返回码': 'rtn_code'
        , '下单IP归属地': 'order_loc'
        , '用户支付IP归属地': 'pay_loc'
        , '预留手机号归属地': 'mobile_loc'
                          }, inplace=True)
    rtn_df["batch_no"] = time.strftime("%Y%m%d%H%M%S")
    rtn_df['trx_amount'] = rtn_df['trx_amount'].astype(float)
    return rtn_df


def get_cust_trx_hist(parm_csv_floder, parm_csv_file_list):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name, sys._getframe().f_lineno)
    rst = pd.DataFrame({})
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_floder, '/', csv_file))
        print('csv_file_path =', csv_file_path)

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding='gbk', chunksize=5000, iterator=True, dtype=str)

        # 3. 数据处理
        df_trx_detail = get_trx_detail(reader)

        rst = rst.append(df_trx_detail, ignore_index=True)
    print('rst \n', rst.tail())
    return rst
