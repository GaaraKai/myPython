import pandas as pd
import time
import os
import sys
import csv


def get_csv_dict(parm_path):
    return csv.DictReader(open(parm_path, 'r'))


def get_cs_rule(parm_conf_path):
    """
    Read Conf Files & Return Call Rule List

    Parameters:
    None.

    Returns:
    Type: DataFrame
    Name: df_rule_list

    Raises:
    IOError: An error occurred accessing the bigtable.Table object.
    """
    opened_file =open(parm_conf_path, 'r')
    csv_read_result = csv.reader(opened_file)
    print(csv_read_result)
    df_rule_list = pd.DataFrame({})
    for line in csv_read_result:
        for i in range(0, len(line)):
            risk_no = line[i]
            df_rule = pd.DataFrame({'rule_no': [risk_no]})
            df_rule_list = df_rule_list.append(df_rule)
    df_rule_list.reset_index()
    return df_rule_list


def get_trx_list(parm_csv_reader):
    rst = pd.DataFrame({})
    for line in parm_csv_reader:
        # if 1 < parm_csv_reader.line_num < 3:
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
                df_rule_list = pd.DataFrame({'risk_no': [risk_no],
                                             'inst_id': [inst_id],
                                             'inst_trace': [inst_trace],
                                             'mer_id': [mer_id],
                                             'mer_name': [mer_name],
                                             'prod_id': [prod_id],
                                             'prod_name': [prod_name],
                                             'order_id': [order_id],
                                             'id_no': [id_no],
                                             'mobile_no': [mobile_no],
                                             'card_no': [card_no],
                                             'trx_amount': [trx_amount],
                                             'trx_status': [trx_status],
                                             'rule_no': [rule_no],
                                             'cre_date': [cre_date],
                                             'cre_time': [cre_time],
                                             'order_ip': [order_ip],
                                             'pay_ip': [pay_ip],
                                             'order_loc': [order_loc],
                                             'pay_loc': [pay_loc],
                                             'mobile_loc': [mobile_loc],
                                             'batch_no': [batch_no]})
                rst = rst.append(df_rule_list)
    return rst


def get_cs_call(parm_csv_floder, parm_csv_file_list):
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
        df_trx_list = get_trx_list(csv_reader)
        # print('df_trx_list\n',df_trx_list)


        # 4.Drop Duplicated Transactions by 'inst_trace' & 'rule_no'
        df_trx_list = df_trx_list.drop_duplicates(subset=['inst_trace', 'rule_no'], keep='first')

        # 5. Get CS_CALL Rules as df_cs_rule
        df_cs_rule = get_cs_rule(conf_path)

        # 6. Merge df_trx_list & df_cs_rule to get REAL CA_CALL Transactions
        merge_df_rule_list = pd.merge(left=df_trx_list,
                                      right=df_cs_rule, how='inner',
                                      left_on='rule_no',
                                      right_on='rule_no')
        print('merge_df_rule_list =', merge_df_rule_list.iloc[:, 0].size)
        print('merge_df_rule_list \n', merge_df_rule_list)
        rst = rst.append(merge_df_rule_list, ignore_index=True)
    print('111111\n',rst)
    print('rst =', rst.iloc[:, 0].size)
    return rst


# 当前文件的路径
pyfile_path = os.getcwd()
# 当前文件的父路径
father_path = os.path.abspath(os.path.dirname(pyfile_path) + os.path.sep + ".")
# 配置文件路径
conf_path = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") \
            + '\\docs\\conf\\cs_call_rule_list.txt'
