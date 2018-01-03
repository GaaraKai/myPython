import pandas as pd
import time
import os
import sys
import csv


# rc_trx_detail


def get_mer_id(param_str):
    return param_str[0:param_str.index('(')]


def get_mer_name(param_str):
    return param_str[param_str.index('(')+1:-1]


def get_cust_trx_hist(csv_reader):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name, sys._getframe().f_lineno)
    rtn_df = pd.DataFrame({})
    # qwe=csv.reader(open('D:/github_program/myPython/docs/csvfiles/ds_stat/171218095044__P14M000K.csv','r'))
    # for line in qwe:
    #     if 0 < qwe.line_num <= 2:
    #         for i in range(0, len(line)):
    #             print('line[',i,'] = ' , line[i]  )
    #
    # sys.exit(0)
    for line in csv_reader:
        # if 1 < csv_reader.line_num < 3:
            inst_id = line['机构号']  # 0
            inst_trace = line['机构请求流水']  # 1
            inner_trade_id = line['内部订单号']  # 2
            order_id = line['商户订单号']  # 5
            order_date = line['商户订单日期']  # 6

            mer_id = get_mer_id(line['主商户号'])   # 7
            mer_name = get_mer_name(line['主商户号'])   # 7
            order_ip = line['下单IP']  # 14
            pay_ip = line['用户支付IP']  # 15
            td_device = line['同盾设备指纹']  # 16

            zy_device = line['自研设备指纹']  # 17
            id_no = line['证件/机构代号']  # 19
            trx_amount = float(line['扣款(元)'])  # 21
            pay_date = line['支付日期']  # 23
            plat_date = line['平台日期']  # 25 -

            plat_time = line['平台时间']  # 26
            card_type = line['卡类型']  # 31
            trx_status = line['交易状态']  # 34
            mobile_no = line['手机号']  # 35
            recv_card_no = line['收款方卡号']  # 36

            recv_name = line['收款方姓名/公司名称']  # 37
            prod_id = line['支付产品编号'][0:8]  # 38
            prod_name = line['支付产品编号'][9:-1]  # 38
            issue_bank = line['发卡行']  # 40
            bank_channel = line['银行通道编号']  # 41

            bank_rtn_trace = line['银行返回流水']  # 53
            gateway_trace = line['支付网关去银行流水']  # 54
            card_no = line['卡号（脱敏后卡号前六后四）']  # 60
            card_holder = line['持卡人']  # 61
            rtn_code = line['返回码']  # 62

            order_loc = line['下单IP归属地']  # 64
            pay_loc = line['用户支付IP归属地']  # 65
            mobile_loc = line['预留手机号归属地']  # 67
            batch_no = time.strftime("%Y%m%d%H%M%S")

            df_trx_detail = pd.DataFrame({ 'inst_id': [inst_id]
                                         , 'inst_trace': [inst_trace]
                                         , 'inner_trade_id': [inner_trade_id]
                                         , 'order_id': [order_id]
                                         , 'order_date': [order_date]
                                         , 'mer_id': [mer_id]
                                         , 'mer_name': [mer_name]
                                         , 'order_ip': [order_ip]
                                         , 'pay_ip': [pay_ip]
                                         , 'td_device': [td_device]
                                         , 'zy_device': [zy_device]
                                         , 'id_no': [id_no]
                                         , 'trx_amount': [trx_amount]
                                         , 'pay_date': [pay_date]
                                         , 'plat_date': [plat_date]
                                         , 'plat_time': [plat_time]
                                         , 'card_type': [card_type]
                                         , 'trx_status': [trx_status]
                                         , 'mobile_no': [mobile_no]
                                         , 'recv_card_no': [recv_card_no]
                                         , 'recv_name': [recv_name]
                                         , 'prod_id': [prod_id]
                                         , 'prod_name': [prod_name]
                                         , 'issue_bank': [issue_bank]
                                         , 'bank_channel': [bank_channel]
                                         , 'bank_rtn_trace': [bank_rtn_trace]
                                         , 'gateway_trace': [gateway_trace]
                                         , 'card_no': [card_no]
                                         , 'card_holder': [card_holder]
                                         , 'rtn_code': [rtn_code]
                                         , 'order_loc': [order_loc]
                                         , 'pay_loc': [pay_loc]
                                         , 'mobile_loc': [mobile_loc]
                                         , 'batch_no': [batch_no]})
            rtn_df = rtn_df.append(df_trx_detail)
    # Get All Transactions as rtn_df
    rtn_df.reset_index()
    print('rtn_df \n', rtn_df)
    return rtn_df

#
# parm_path = r"D:/github_program/myPython/docs/csvfiles/ds_stat/171218095044__P14M000K.csv"
# path = csv.DictReader(open(parm_path, 'r'))
#
# get_cust_trx_hist(path)
