import pandas as pd
import time
import os
import sys
import csv




def get_csv_dict(parm_path):
    return csv.DictReader(open(parm_path, 'r'))


def get_warnmgt_list(parm_csv_reader):
    rst = pd.DataFrame({})
    for line in parm_csv_reader:
        # if 1 < csv_reader.line_num < 4:
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
            df_warnmgt_list = pd.DataFrame({'risk_no': [risk_no],
                                            'mer_id': [mer_id],
                                            'mer_name': [mer_name],
                                            'prod_id': [prod_id],
                                            'prod_name': [prod_name],
                                            'mobile_no': [mobile_no],
                                            'card_no': [card_no],
                                            'issue_bank': [issue_bank],
                                            'id_no': [id_no],
                                            'trx_amount': [trx_amount],
                                            'proc_status': [proc_status],
                                            'risk_rank': [risk_rank],
                                            'order_id': [order_id],
                                            'user_id': [user_id],
                                            'biz_type': [biz_type],
                                            'cre_date': [cre_date],
                                            'cre_time': [cre_time],
                                            'remark': [remark],
                                            'rule_no': [rule_no],
                                            'trx_status': [trx_status],
                                            'lst_proc_time': [lst_proc_time],
                                            'lst_proc_opr': [lst_proc_opr],
                                            'order_ip': [order_ip],
                                            'pay_ip': [pay_ip],
                                            'order_loc': [order_loc],
                                            'pay_loc': [pay_loc],
                                            'mobile_loc': [mobile_loc],
                                            'batch_no': [batch_no]})
            rst = rst.append(df_warnmgt_list)
    return rst


def get_cs_warn_mgt(parm_csv_floder, parm_csv_file_list):
    """
    Data Preparation from a CSV Files.

        get_cs_rule from a CSV Files..

    Parameters:
        csv_reader: An open Bigtable Table instance.

    Returns:
        Transactions which Fire CS_CALL rules
        Type: DataFrame
        Na  me: merge_df_rule_list

    Raises:
        IOError: An error occurred accessing the bigtable.Table object. ?
    """
    rst = pd.DataFrame({})
    for csv_file in parm_csv_file_list:
        # 1. Get csv files path by csv floder & csv file list
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_floder, '/', csv_file))
        print('csv_file_path =', csv_file_path)

        # 2. Get csv dict by csv file path
        csv_reader = get_csv_dict(csv_file_path)
        print(os.path.basename(__file__), sys._getframe().f_code.co_name, 'line_no =', sys._getframe().f_lineno)

        # 3. Process Csv Dict to Get All CA_CALL Transactions as df_trx_list
        df_warnmgt_list = get_warnmgt_list(csv_reader)
        print('df_warnmgt_list =', df_warnmgt_list.iloc[:, 0].size)
        rst = rst.append(df_warnmgt_list)
    print('rst =', rst.iloc[:, 0].size)
    return rst

# def get_cs_warn_mgt_bak(csv_reader):
#     print(os.path.basename(__file__), sys._getframe().f_code.co_name,
#           sys._getframe().f_lineno, 'csv_reader = ', csv_reader)
#     df_append = pd.DataFrame({})
#     for line in csv_reader:
#         # if 1 < csv_reader.line_num < 4:
#             # cs_warn_mgt
#             risk_no = line['风险号']
#             mer_id = line['商户号']
#             mer_name = line['商户名称'].replace(' ', '')
#             prod_id = line['产品号']
#             prod_name = line['产品名']
#             mobile_no = line['手机号']
#             card_no = line['银行卡号']
#             issue_bank = line['发卡行']
#             id_no = line['证件号']
#             trx_amount = float(line['交易金额'])
#             proc_status = line['处理状态']
#             risk_rank = line['风险级别']
#             order_id = line['商户订单号']
#             user_id = line['用户ID']
#             biz_type = line['业务类型']
#             cre_date = line['交易时间'][0:10]
#             cre_time = line['交易时间'][-8:]
#             remark = line['备注']
#             rule_no = line['触发规则'].split(':')[0]
#             trx_status = line['交易状态']
#             lst_proc_time = line['最后处理时间']
#             lst_proc_opr = line['最后处理人']
#             order_ip = line['下单IP']
#             pay_ip = line['用户支付IP']
#             order_loc = line['下单IP归属地']
#             pay_loc = line[' 用户支付IP归属地']
#             mobile_loc = line['预留手机号归属地']
#             batch_no = time.strftime("%Y%m%d%H%M%S")
#             df = pd.DataFrame({'risk_no': [risk_no], 'mer_id': [mer_id], 'mer_name': [mer_name] \
#                                   , 'prod_id': [prod_id], 'prod_name': [prod_name], 'mobile_no': [mobile_no] \
#                                   , 'card_no': [card_no], 'issue_bank': [issue_bank], 'id_no': [id_no] \
#                                   , 'trx_amount': [trx_amount], 'proc_status': [proc_status], 'risk_rank': [risk_rank] \
#                                   , 'order_id': [order_id], 'user_id': [user_id], 'biz_type': [biz_type] \
#                                   , 'cre_date': [cre_date], 'cre_time': [cre_time], 'remark': [remark] \
#                                   , 'rule_no': [rule_no], 'trx_status': [trx_status], 'lst_proc_time': [lst_proc_time] \
#                                   , 'lst_proc_opr': [lst_proc_opr], 'order_ip': [order_ip], 'pay_ip': [pay_ip] \
#                                   , 'order_loc': [order_loc], 'pay_loc': [pay_loc], 'mobile_loc': [mobile_loc],
#                                'batch_no': [batch_no]})
#             df_append = df_append.append(df)
#     df_append.reset_index()
#     # print('df_append\n',df_append)
#     return df_append


