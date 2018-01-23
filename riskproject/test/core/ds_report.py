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


def get_device_trx_hist(parm_csv_floder, parm_csv_file_list):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name, sys._getframe().f_lineno)
    father_path = os.path.abspath(os.path.dirname(parm_csv_floder) + os.path.sep + ".")
    rst_dir = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\rst\\'


    rst_path = parm_csv_floder
    rst = pd.DataFrame({})
    cap_trx_cnt = 0
    total_trx_amount = .0
    date_range = 106
    mer_id_srs = pd.Series([])
    # mer_id_list = pd.DataFrame({})
    mean_list = []
    # parm_csv_file_list = ["NO 1_td_1","NO 1_td_1"]
    # gc.disable()
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
    # total_trx_amount = round(total_trx_amount / 100000000, 2)
    # print("total_trx_amount:", total_trx_amount)
    # print("cap_trx_cnt:", cap_trx_cnt)
    # mer_id_cnt = np.array(rst["mer_id"].unique()).size
    # prod_id_cnt= pd.DataFrame(np.array(rst["prod_id"].unique()).tolist()).dropna(axis=0,how='any').size
    # device_id_cnt = pd.DataFrame(np.array(rst["td_device"].unique()).tolist()).dropna(axis=0, how='any').size
    # device_id_cnt_daily = round(device_id_cnt / date_range, 2)
    # print("mer_id_cnt:", mer_id_cnt)
    # print("prod_id_cnt:", prod_id_cnt)
    # print("device_id_cnt:", device_id_cnt)
    # print("device_id_cnt_daily:", device_id_cnt_daily)
    print("////////////////////////////////////////////")

    print("////////////////////////////////////////////")
    # prod_id_details_path = rst_dir + "prod_id_details" + ".csv"
    # pd.DataFrame(rst["prod_id"].value_counts()).reset_index().to_csv(prod_id_details_path, index=False)
    print("////////////////////////////////////////////")

    print("////////////////////////////////////////////")
    # mer_id_details_path = rst_dir + "mer_id_details" + ".csv"
    # pd.DataFrame(rst["mer_id"].value_counts()).reset_index().head(10).to_csv(mer_id_details_path, index=False)
    print("////////////////////////////////////////////")

    print("////////////////////////////////////////////")
    # td_device_details_path = rst_dir + "td_device_details" + ".csv"
    # pd.DataFrame(rst["td_device"].value_counts()).reset_index().head(10).to_csv(td_device_details_path, index=False)
    print("////////////////////////////////////////////")

    # print(rst[rst["td_device"] == "fffe9b172156e2db230697c3e7cd383f"])

    td_device_piv = pd.pivot_table(rst,
                                   index=["td_device"],
                                   values=["trx_amount"],
                                   aggfunc=[len,np.sum])
    # td_device_piv_path = rst_dir + "td_device_piv" + ".csv"
    # td_device_piv.head(10).to_csv(td_device_piv_path, index=False)
    print(td_device_piv.dtypes)

    xx = pd.DataFrame({"td_device": ["00003437192547be98c8c20fe990c1b0",
                            "0001cfeba9cd879e361d23c5bde8706f",
                            "00020988c4de409aace44cfc44384bd5"]})
    # print(xx)

    # yy = pd.merge(td_device_piv, xx, how="inner",on="td_device")
    # print(yy)





    del rst
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
    get_device_trx_hist(csv_floder, csv_file_list)
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



