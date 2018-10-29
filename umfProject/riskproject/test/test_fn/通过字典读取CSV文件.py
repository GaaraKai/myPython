import csv
import datetime
import time
import pandas as pd


a =open('D:/github_program/myPython/docs/csvfiles/ds_stat/180102102553__P14M0000-1225-1227.csv','r')
csv_read = csv.reader(open('D:/github_program/myPython/docs/csvfiles/ds_stat/180102102553__P14M0000-1225-1227.csv','r'))
csv_dict = csv.DictReader(open('D:/github_program/myPython/docs/csvfiles/ds_stat/180102102553__P14M0000-1225-1227.csv', 'r'))



def get_mer_id(param_str):
    return param_str[0:param_str.index('(')]


def get_mer_name(param_str):
    return param_str[param_str.index('(')+1:-1]

def fn1(parm_csv_dict):
    rst = pd.DataFrame({})
    for line in parm_csv_dict:
        if 1 < parm_csv_dict.line_num < cnt:
            inst_id = line['机构号']  # 0
            inst_trace = line['机构请求流水']  # 1
            inner_trade_id = line['内部订单号']  # 2
            order_id = line['商户订单号']  # 5
            order_date = line['商户订单日期']  # 6

            mer_id = get_mer_id(line['主商户号'])  # 7
            mer_name = get_mer_name(line['主商户号'])  # 7
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

            df_trx_detail = pd.DataFrame({'inst_id': [inst_id],
                                          'inst_trace': [inst_trace],
                                          'inner_trade_id': [inner_trade_id],
                                          'order_id': [order_id],
                                          'order_date': [order_date],
                                          'mer_id': [mer_id],
                                          'mer_name': [mer_name],
                                          'order_ip': [order_ip],
                                          'pay_ip': [pay_ip],
                                          'td_device': [td_device],
                                          'zy_device': [zy_device],
                                          'id_no': [id_no],
                                          'trx_amount': [trx_amount],
                                          'pay_date': [pay_date],
                                          'plat_date': [plat_date],
                                          'plat_time': [plat_time],
                                          'card_type': [card_type],
                                          'trx_status': [trx_status],
                                          'mobile_no': [mobile_no],
                                          'recv_card_no': [recv_card_no],
                                          'recv_name': [recv_name],
                                          'prod_id': [prod_id],
                                          'prod_name': [prod_name],
                                          'issue_bank': [issue_bank],
                                          'bank_channel': [bank_channel],
                                          'bank_rtn_trace': [bank_rtn_trace],
                                          'gateway_trace': [gateway_trace],
                                          'card_no': [card_no],
                                          'card_holder': [card_holder],
                                          'rtn_code': [rtn_code],
                                          'order_loc': [order_loc],
                                          'pay_loc': [pay_loc],
                                          'mobile_loc': [mobile_loc],
                                          'batch_no': [batch_no]})
            rst = rst.append(df_trx_detail)
    # print('rst1 \n ',rst)
    print('rst1 = ', rst.iloc[:, 0].size)

def fn2(parm_csv_dict):
    rst = pd.DataFrame({})
    for line in parm_csv_dict:
        # if 1 < parm_csv_dict.line_num < cnt:
            inst_id = line[0]  # 0
            inst_trace = line[1]  # 1
            inner_trade_id = line[2]  # 2
            order_id = line[5]  # 5
            order_date = line[6]  # 6

            mer_id = get_mer_id(line[7])  # 7
            mer_name = get_mer_name(line[7])  # 7
            order_ip = line[14]  # 14
            pay_ip = line[15]  # 15
            td_device = line[16]  # 16

            zy_device = line[17]  # 17
            id_no = line[19]  # 19
            trx_amount = float(line[21])  # 21
            pay_date = line[23]  # 23
            plat_date = line[25]  # 25 -

            plat_time = line[26]  # 26
            card_type = line[31]  # 31
            trx_status = line[34]  # 34
            mobile_no = line[35]  # 35
            recv_card_no = line[36]  # 36

            recv_name = line[37]  # 37
            prod_id = line[38][0:8]  # 38
            prod_name = line[38][9:-1]  # 38
            issue_bank = line[40]  # 40
            bank_channel = line[41]  # 41

            bank_rtn_trace = line[53]  # 53
            gateway_trace = line[54]  # 54
            card_no = line[60]  # 60
            card_holder = line[61]  # 61
            rtn_code = line[62]  # 62

            order_loc = line[64]  # 64
            pay_loc = line[65]  # 65
            mobile_loc = line[67]  # 67
            batch_no = time.strftime("%Y%m%d%H%M%S")

            df_trx_detail = pd.DataFrame({'inst_id': [inst_id],
                                          'inst_trace': [inst_trace],
                                          'inner_trade_id': [inner_trade_id],
                                          'order_id': [order_id],
                                          'order_date': [order_date],
                                          'mer_id': [mer_id],
                                          'mer_name': [mer_name],
                                          'order_ip': [order_ip],
                                          'pay_ip': [pay_ip],
                                          'td_device': [td_device],
                                          'zy_device': [zy_device],
                                          'id_no': [id_no],
                                          'trx_amount': [trx_amount],
                                          'pay_date': [pay_date],
                                          'plat_date': [plat_date],
                                          'plat_time': [plat_time],
                                          'card_type': [card_type],
                                          'trx_status': [trx_status],
                                          'mobile_no': [mobile_no],
                                          'recv_card_no': [recv_card_no],
                                          'recv_name': [recv_name],
                                          'prod_id': [prod_id],
                                          'prod_name': [prod_name],
                                          'issue_bank': [issue_bank],
                                          'bank_channel': [bank_channel],
                                          'bank_rtn_trace': [bank_rtn_trace],
                                          'gateway_trace': [gateway_trace],
                                          'card_no': [card_no],
                                          'card_holder': [card_holder],
                                          'rtn_code': [rtn_code],
                                          'order_loc': [order_loc],
                                          'pay_loc': [pay_loc],
                                          'mobile_loc': [mobile_loc],
                                          'batch_no': [batch_no]})
            rst = rst.append(df_trx_detail)
    # print('rst2 \n ',rst)
    print('rst2 = ', rst.iloc[:, 0].size)


cnt = 10000
start_time = datetime.datetime.now()
fn1(csv_dict)
# fn2(csv_read)

end_time = datetime.datetime.now()
print('start_time =', start_time)
print('end_time   =', end_time)
print('diff_Time =', end_time - start_time)