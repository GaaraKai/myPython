import pandas as pd
import time
import os
import sys


# ca_call


def get_cs_call(csv_reader):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name,
          sys._getframe().f_lineno, 'csv_reader = ', csv_reader)
    df_append = pd.DataFrame({})
    for line in csv_reader:
        if 1 < csv_reader.line_num < 40000:
            # cs_warn_mgt
            for i in range(0, len(line['触发规则'].split(';'))):
                risk_no = line['风险号']
                inst_id = line['机构号']
                inst_trace = line['机构请求流水']
                mer_id = line['商户号']
                mer_name = line['商户名称'].replace(' ', '')
                prod_id = line['产品号']
                prod_name = line['产品名']
                order_id = line['商户订单号']
                id_no = line['证件号/机构代码']
                mobile_no = line['手机号']
                card_no = line['银行卡号']
                trx_amount = float(line['金额（元）'])
                trx_status = line['交易状态']
                rule_no = line['触发规则'].split(';')[i][0:4]
                cre_date = line['创建时间'][0:10]
                cre_time = line['创建时间'][-8:]
                order_ip = line['下单IP']
                pay_ip = line['用户支付IP']
                order_loc = line['下单IP归属地']
                pay_loc = line['用户支付IP归属地']
                mobile_loc = line['预留手机号归属地']
                batch_no = time.strftime("%Y%m%d%H%M%S")
                df = pd.DataFrame({'risk_no': [risk_no], 'inst_id': [inst_id], 'inst_trace': [inst_trace] \
                                      , 'mer_id': [mer_id], 'mer_name': [mer_name], 'prod_id': [prod_id] \
                                      , 'prod_name': [prod_name], 'order_id': [order_id], 'id_no': [id_no] \
                                      , 'mobile_no': [mobile_no], 'card_no': [card_no], 'trx_amount': [trx_amount] \
                                      , 'trx_status': [trx_status], 'rule_no': [rule_no], 'cre_date': [cre_date] \
                                      , 'cre_time': [cre_time], 'order_ip': [order_ip], 'pay_ip': [pay_ip] \
                                      , 'order_loc': [order_loc], 'pay_loc': [pay_loc], 'mobile_loc': [mobile_loc] \
                                      , 'batch_no': [batch_no]})
                df_append = df_append.append(df)
    df_append.reset_index()
    # print('df_append\n',df_append)
    return df_append

