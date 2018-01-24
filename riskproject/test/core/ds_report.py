import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os
import sys
import tkinter.filedialog as tf
import tkinter.messagebox as tm
import traceback
import sqlalchemy as sq
import gc
import numpy as np

import shutil
import time
import math


def get_trx_detail(parm_reader):
    rtn_df = pd.DataFrame({})
    for chunk in parm_reader:
        rtn_df = rtn_df.append(chunk)
        # print("rtn_df.shape = ", rtn_df.shape)
    rtn_df.drop(["Unnamed: 15"], axis=1, inplace=True)

    rtn_df.rename(columns={'机构号': 'inst_id'
        , '机构请求流水': 'inst_trace'
        , '内部订单号': 'inner_trade_id'
        , '商户订单号': 'order_id'
        , '主商户号': 'mer_id'
        , '下单IP': 'order_ip'
        , '用户支付IP': 'pay_ip'
        , '同盾设备指纹': 'td_device'
        , '证件机构代号': 'id_no'
        , '扣款（元）': 'trx_amount'
        , '平台日期': 'plat_date'
        , '平台时间': 'plat_time'
        , '手机号': 'mobile_no'
        , '支付产品编号': 'prod_id'
        , '收款方姓名/公司名称': 'recv_name'
        , '持卡人': 'card_holder'}, inplace=True)
    rtn_df["batch_no"] = time.strftime("%Y%m%d%H%M%S")
    rtn_df['trx_amount'] = rtn_df['trx_amount'].astype(float)
    rtn_df['trx_amount'] = rtn_df['trx_amount'] / 100
    # print(rtn_df.tail())
    # print("ALL rtn_df.shape = ", rtn_df.shape)

    return rtn_df


def get_device_report(parm_csv_floder, parm_csv_file_list):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name, sys._getframe().f_lineno)
    father_path = os.path.abspath(os.path.dirname(parm_csv_floder) + os.path.sep + ".")
    rst_dir = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\rst\\'
    rst = pd.DataFrame({})
    td_device_rst = pd.DataFrame({})
    cap_trx_cnt = 0
    total_trx_amount = .0
    date_range = 106
    # gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)
    gc.collect()
    gc.disable()
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_floder, '/', csv_file))
        print('csv_file_path =', csv_file_path)
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/todo/NO 1_td_1"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding='utf-8', chunksize=5000, iterator=True, sep="|",dtype=str)

        # 3. 数据处理
        df_trx_detail = get_trx_detail(reader)
        print("df_trx_detail =", df_trx_detail.shape)
        total_trx_amount = total_trx_amount + df_trx_detail["trx_amount"].sum()
        cap_trx_cnt = cap_trx_cnt + df_trx_detail.shape[0]
        rst = rst.append(df_trx_detail, ignore_index=True)
        reader.close()
        del reader
        del df_trx_detail
    gc.enable()  # re-enable garbage collection
    gc.collect()
    print("////////////////////////////////////////////")
    print("Historical Data:")
    total_trx_amount = round(total_trx_amount / 100000000, 2)
    print("Total Device ID Transaction Count:", cap_trx_cnt)
    print("Total Device ID Transaction Amount:", total_trx_amount)
    mer_id_cnt = np.array(rst["mer_id"].unique()).size
    prod_id_cnt= pd.DataFrame(np.array(rst["prod_id"].unique()).tolist()).dropna(axis=0,how='any').size
    device_id_cnt = pd.DataFrame(np.array(rst["td_device"].unique()).tolist()).dropna(axis=0, how='any').size
    device_id_cnt_daily = round(device_id_cnt / date_range, 2)
    print("Total Device ID Merchant Count:", mer_id_cnt)
    print("Total Device ID Product Count:", prod_id_cnt)
    print("Total Device ID Transaction Count:", device_id_cnt)
    print("Daily Total Device ID Transaction Count:", device_id_cnt_daily)
    print("////////////////////////////////////////////")

    print("////////////////////////////////////////////")
    prod_id_details_path = rst_dir + "prod_id_details" + ".csv"
    pd.DataFrame(rst["prod_id"].value_counts()).reset_index().to_csv(prod_id_details_path, index=False)
    print("1.Result Create Finished:", prod_id_details_path)
    print("////////////////////////////////////////////")

    print("////////////////////////////////////////////")
    mer_id_details_path = rst_dir + "mer_id_details" + ".csv"
    pd.DataFrame(rst["mer_id"].value_counts()).reset_index().head(10).to_csv(mer_id_details_path, index=False)
    print("2.Result Create Finished:", mer_id_details_path)
    print("////////////////////////////////////////////")

    print("////////////////////////////////////////////")
    td_device_details_path = rst_dir + "td_device_details" + ".csv"
    pd.DataFrame(rst["td_device"].value_counts()).reset_index().head(10).to_csv(td_device_details_path, index=False)
    print("3.Result Create Finished:", td_device_details_path)
    print("////////////////////////////////////////////")

    print("////////////////////////////////////////////")
    td_device_cnt = pd.pivot_table(rst,index=["td_device"],values=["trx_amount"],aggfunc=len)
    td_device_sum = pd.pivot_table(rst,index=["td_device"],values=["trx_amount"],aggfunc=np.sum)
    td_device_piv = pd.DataFrame(td_device_cnt).reset_index()
    df_td_device_sum = pd.DataFrame(td_device_sum).reset_index(drop=True)
    td_device_piv["sum"] = df_td_device_sum["trx_amount"]
    td_device_piv.rename(columns={"trx_amount": "cnt"}, inplace=True)
    print("////////////////////////////////////////////")

    print("********************************************")
    ready_to_add_list = pd.read_csv("C:/Users/wangrongkai/Desktop/123/ready_to_add.csv")["TD_DEVICEID"].tolist()
    print("The Total Count of TD_device GRAY List which ready to import:", len(ready_to_add_list))
    # ready_to_add_list = ["00003437192547be98c8c20fe990c1b0","00020988c4de409aace44cfc44384bd5","00029aa3204bb47d1288b7e5ee37be89"]
    print("********************************************")

    print("////////////////////////////////////////////")
    for single_td_device in td_device_piv["td_device"]:
        if single_td_device in ready_to_add_list:
            row = td_device_piv[td_device_piv["td_device"] == single_td_device]
            td_device_rst = td_device_rst.append(row)
    td_device_rst["cnt_daily"] = round(td_device_rst["cnt"] / date_range, 2)
    td_device_rst["sum_daily"] = round(td_device_rst["sum"] / date_range, 2)
    td_device_rst = td_device_rst.sort_values(by='sum_daily', axis=0, ascending=False)
    td_device_rst_path = rst_dir + "td_device_rst" + ".csv"
    td_device_rst.to_csv(td_device_rst_path)
    print("4.Result Create Finished:", td_device_rst_path)
    print("The Count of In the TD_device GRAY List & exist transaction in historical data:",
          td_device_rst.shape[0])
    print("////////////////////////////////////////////")

    print("////////////////////////////////////////////")
    print("Based on historical data, the NEGATIVE Effects if import TD_device GRAY List is:")
    total_intercept_cnt = int(td_device_rst["cnt"].sum())
    total_intercept_amt = td_device_rst["sum"].sum()
    daily_intercept_cnt = int(td_device_rst["cnt_daily"].sum())
    daily_intercept_amt = td_device_rst["sum_daily"].sum()
    print("Total Count of Gray List interception will be increase:", total_intercept_cnt)
    print("Total Amount of Gray List interception will be increase:", total_intercept_amt)
    print("Daily Total Count of Gray List interception will be increase:", daily_intercept_cnt)
    print("Daily Total Amount of Gray List interception will be increase:", daily_intercept_amt)
    print("TOP10 TD_device:\n", td_device_rst.head(10))
    print("////////////////////////////////////////////")
    del rst
    del td_device_piv
    del td_device_cnt
    del td_device_sum
    del td_device_rst
    del df_td_device_sum
    return 0

def get_csv_floder():
    global csv_biz_type
    father_path = os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep + ".")
    default_dir = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\csvfiles\\'
    # csv_floder = "D:/github_program/myPython/docs/csvfiles/todo/"
    csv_floder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=(os.path.expanduser(default_dir)))
    if len(csv_floder) != 0:
        csv_biz_type = csv_floder.split('/')[-1]
        print('csv_biz_type = ', csv_biz_type)
    else:
        tm.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)

    return csv_floder


def main_process(parm_csv_floder):
    """
        Data Preparation From CSV File & Insert into DB

        Parameters:
          parm_csv_floder: CSV file path

        Returns:
          None

        Raises:
          IOError: An error occurred accessing the bigtable.Table object.
        """
    csv_file_list = os.listdir(parm_csv_floder)
    print('csv_file_list = ', csv_file_list)
    get_device_report(csv_floder, csv_file_list)
    print('Main Processing Have Done...')


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    csv_floder = get_csv_floder()
    print('csv_floder = ', csv_floder)
    main_process(csv_floder)
    end_time = datetime.datetime.now()
    print('start_time =', start_time)
    print('end_time   =', end_time)
    print('diff_Time =', end_time - start_time)
    print('System Processing Done...')



