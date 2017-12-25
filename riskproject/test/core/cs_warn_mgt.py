import pandas as pd
import time
import os
import sys


# cs_warn_mgt

def get_cs_warn_mgt(csv_reader):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name,
          sys._getframe().f_lineno, 'csv_reader = ', csv_reader)
    df_append = pd.DataFrame({})
    for line in csv_reader:
        if 1 < csv_reader.line_num < 4:
            # cs_warn_mgt
            risk_no = line['风险号']
            mer_id = line['商户号']
            mer_name = line['商户名称'].replace(' ', '')
            prod_id = line['产品号']
            prod_name = line['产品名']
            mobile_no = line['手机号']
            card_no = line['银行卡号']
            issue_bank = line['发卡行']
            id_no = line['证件号']
            trx_amount = float(line['交易金额'])
            proc_status = line['处理状态']
            risk_rank = line['风险级别']
            order_id = line['商户订单号']
            user_id = line['用户ID']
            biz_type = line['业务类型']
            cre_date = line['交易时间'][0:10]
            cre_time = line['交易时间'][-8:]
            remark = line['备注']
            rule_no = line['触发规则'].split(':')[0]
            trx_status = line['交易状态']
            lst_proc_time = line['最后处理时间']
            lst_proc_opr = line['最后处理人']
            order_ip = line['下单IP']
            pay_ip = line['用户支付IP']
            order_loc = line['下单IP归属地']
            pay_loc = line[' 用户支付IP归属地']
            mobile_loc = line['预留手机号归属地']
            batch_no = time.strftime("%Y%m%d%H%M%S")
            df = pd.DataFrame({'risk_no': [risk_no], 'mer_id': [mer_id], 'mer_name': [mer_name] \
                                  , 'prod_id': [prod_id], 'prod_name': [prod_name], 'mobile_no': [mobile_no] \
                                  , 'card_no': [card_no], 'issue_bank': [issue_bank], 'id_no': [id_no] \
                                  , 'trx_amount': [trx_amount], 'proc_status': [proc_status], 'risk_rank': [risk_rank] \
                                  , 'order_id': [order_id], 'user_id': [user_id], 'biz_type': [biz_type] \
                                  , 'cre_date': [cre_date], 'cre_time': [cre_time], 'remark': [remark] \
                                  , 'rule_no': [rule_no], 'trx_status': [trx_status], 'lst_proc_time': [lst_proc_time] \
                                  , 'lst_proc_opr': [lst_proc_opr], 'order_ip': [order_ip], 'pay_ip': [pay_ip] \
                                  , 'order_loc': [order_loc], 'pay_loc': [pay_loc], 'mobile_loc': [mobile_loc],
                               'batch_no': [batch_no]})
            df_append = df_append.append(df)
    df_append.reset_index()
    # print('df_append\n',df_append)
    return df_append


