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
    global START_DATE
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
    rtn_df["plat_date"] = pd.to_datetime(rtn_df["plat_date"])
    rtn_df = rtn_df[rtn_df["plat_date"] >= START_DATE]

    return rtn_df


def susp_device_stat(parm_df):
    global RST_PATH, ID_HURDLE
    print("****SUSPICIOUS DEVICE ANALYSIS BEGIN****")
    print("\n[1]. ALL TRANSACTION WITH DROP NULL ID_NO:")
    rst_dropna_id_no = parm_df.dropna(axis=0, subset=["id_no"])
    print("SHAPE:", rst_dropna_id_no.shape)  # (274038, 16)
    print("\n[2]. AFTER [1], DROP DUPLICATED TD_DEVICE & ID_NO:")
    rst_dropna_id_no_dup = rst_dropna_id_no.drop_duplicates(["td_device", "id_no"])
    print("SHAPE:", rst_dropna_id_no_dup.shape)  # (90288, 16)
    print("\n[3]. FIND COUNT OF ID_NO BY EVERY DEVICE[TOP5]:")
    devi_with_id_cnt = pd.pivot_table(rst_dropna_id_no_dup, index=["td_device"], values=["id_no"], aggfunc=len) \
        .sort_values(by='id_no', axis=0, ascending=False).reset_index()
    devi_with_id_cnt.rename(columns={"id_no": "id_no_cnt"}, inplace=True)
    print(devi_with_id_cnt.head())
    print("\n[4]. FIND SUSPICIOUS DEVICE WHERE ID HURDLE IS[", ID_HURDLE, "]:")
    susp_device_list = pd.Series(devi_with_id_cnt[devi_with_id_cnt["id_no_cnt"] >= ID_HURDLE]["td_device"]) \
        .tolist()
    if len(susp_device_list) == 0:
        print("NO SUSPICIOUS DEVICE BECAUSE ID HURDLE", ID_HURDLE, "IS TOO HIGH.")
    else:
        print("==>> SUSPICIOUS DEVICE:", susp_device_list)
        print("\n[5]. FIND MERCHANT WHIT SUSPICIOUS DEVICE:")
        mer_with_susp_device = pd.DataFrame(
            rst_dropna_id_no_dup[rst_dropna_id_no_dup["td_device"].isin(susp_device_list)])
        mer_with_susp_device_list = mer_with_susp_device["mer_id"].unique().tolist()
        print("==>> SUSPICIOUS MERCHANT:", mer_with_susp_device_list)
        mer_with_susp_device_detl = pd.pivot_table(mer_with_susp_device,
                                                   values=["inst_id"],
                                                   index=["mer_id", "td_device", "id_no"],
                                                   aggfunc=len).sort_values(by='inst_id', axis=0,
                                                                            ascending=False).reset_index()
        print("\n[6]. FIND SUSPICIOUS DEVICE TRANSACTION DETAILS:")
        print("==>> SUSPICIOUS TRANSACTION[e.g.]:\n", mer_with_susp_device_detl.head())
        print("\n[7]. SAVE SUSPICIOUS DEVICE TRANSACTION DETAILS:")
        mer_with_susp_device_trx_details_path = RST_PATH + "mer_with_susp_device_trx_details" + ".csv"
        # mer_with_susp_device.to_csv(mer_with_susp_device_trx_details_path, index=False)
        print("SAVE TO:", mer_with_susp_device_trx_details_path)
    print("****SUSPICIOUS DEVICE ANALYSIS END**** \n")

    del parm_df


def susp_mer_stat(parm_rst):
    global RST_PATH, TRX_HURDLE
    print("****SUSPICIOUS MERCHANT ANALYSIS BEGIN****")
    print("\n[1]. FIND MERCHANT TRANSACTION COUNT[TOP5]:")
    mer_with_trx_cnt = pd.pivot_table(parm_rst, index=["mer_id"], values=["inst_id"], aggfunc=len) \
        .sort_values(by='inst_id', axis=0, ascending=False).reset_index()
    mer_with_trx_cnt.rename(columns={"inst_id": "trx_cnt"}, inplace=True)
    print(mer_with_trx_cnt.head())

    print("\n[2]. FIND MERCHANT DEVICE COUNT[TOP5]:")
    rst_drop_mer_td = parm_rst.drop_duplicates(["mer_id", "td_device"])
    mer_with_td_device_cnt = pd.pivot_table(rst_drop_mer_td, index=["mer_id"], values=["td_device"],
                                                aggfunc=len) \
        .sort_values(by='td_device', axis=0, ascending=False).reset_index()
    mer_with_td_device_cnt.rename(columns={"td_device": "device_cnt"}, inplace=True)
    print(mer_with_td_device_cnt.head())
    print("\n[3]. MERGE TRANSACTION & DEVICE COUNT BY MERCHANT ID[TOP5]:")
    merge_rst = pd.merge(mer_with_trx_cnt, mer_with_td_device_cnt, how='inner', on=['mer_id'])
    merge_rst["avg_trx_cnt_by_one_device"] = \
        round(merge_rst["trx_cnt"] / merge_rst["device_cnt"], 0)
    print(merge_rst.head())
    print("SHAPE:", merge_rst.shape)
    print("\n[4]. SAVE MERCHANT TRANSACTION & DEVICE COUNT STATISTICAL TABLE:")
    mer_with_susp_device_trx_details_path = RST_PATH + "mer_with_susp_device_trx_details" + ".csv"
    # mer_with_susp_device.to_csv(mer_with_susp_device_trx_details_path, index=False)
    print("SAVE TO:", mer_with_susp_device_trx_details_path)
    print("\n[5] FIND MERCHANT WHERE TRANSACTION COUNT OVER HURDLE[", TRX_HURDLE, "]:")
    key_mer_by_trx_list = pd.Series(
        mer_with_trx_cnt[mer_with_trx_cnt["trx_cnt"] >= TRX_HURDLE]["mer_id"]).tolist()
    if len(key_mer_by_trx_list) == 0:
        print("==>> NO MERCHANT BECAUSE TRX_HURDLE", TRX_HURDLE, "IS TOO HIGH.")
    else:
        print("==>> MONITOR MERCHANT COUNT:", len(key_mer_by_trx_list))
        print("==>> MONITOR MERCHANT DETAILS:", key_mer_by_trx_list)
        print("==>> MONITOR MERCHANT TRANSACTION COUNT[OVER HURDLE]:")
        key_mer_trx_stat = pd.DataFrame(merge_rst[merge_rst["mer_id"].isin(key_mer_by_trx_list)])
        print(key_mer_trx_stat)
    avg_trx_top = 5
    print("\n[6] FIND MERCHANT WHERE AVERAGE TRANSACTION COUNT WITH DEVICE[TOP", avg_trx_top ,"]:")
    key_mer_avg_stat = pd.DataFrame(
        merge_rst.sort_values(by='avg_trx_cnt_by_one_device', axis=0, ascending=False))[0:avg_trx_top]
    key_mer_by_avg_list = pd.Series(key_mer_avg_stat["mer_id"]).tolist()
    print("==>> SUSPICIOUS MERCHANT COUNT:", len(key_mer_by_avg_list))
    print("==>> SUSPICIOUS MERCHANT ID:", key_mer_by_avg_list)
    print("==>> SUSPICIOUS MERCHANT TRANSACTION COUNT[TOP5]:")
    print(key_mer_avg_stat)
    print("****SUSPICIOUS MERCHANT ANALYSIS END**** \n")

    del parm_rst


def overall_rpt(parm_rst):
    global CAP_TRX_CNT, TOT_TRX_AMT, START_DATE
    date_range = 31
    print("****OVERALL REPORT START****")
    print("\n[1]. TOTAL DATA SHAPE:", parm_rst.shape)
    print("\n[2]. STATISTICS RANGE: FROM", START_DATE, "TO NOW")
    total_trx_amount = round(TOT_TRX_AMT / 1000000000, 2)
    print("\n[3]. TOTAL TRANSACTION COUNT:", CAP_TRX_CNT)
    print("\n[4]. TOTAL TRANSACTION AMOUNT:", total_trx_amount, "BILLION")
    mer_id_cnt = np.array(parm_rst["mer_id"].unique())
    prod_id_cnt = pd.DataFrame(np.array(parm_rst["prod_id"].unique()).tolist()).dropna(axis=0, how='any').size
    device_id_cnt = pd.DataFrame(np.array(parm_rst["td_device"].unique()).tolist()).dropna(axis=0, how='any').size
    device_id_cnt_daily = round(device_id_cnt / date_range, 2)
    print("\n[5]. TOTAL MERCHANT COUNT:", mer_id_cnt.size)
    mer_id_details_path = RST_PATH + "mer_id_details" + ".csv"
    # pd.DataFrame(rst["mer_id"].value_counts()).reset_index().head(10).to_csv(mer_id_details_path, index=False)
    print("TOP10 MERCHANT COUNT DETAILS SAVE TO:", mer_id_details_path)
    print("\n[6]. TOTAL PRODUCT COUNT:", prod_id_cnt)
    prod_id_details_path = RST_PATH + "prod_id_details" + ".csv"
    # pd.DataFrame(rst["prod_id"].value_counts()).reset_index().to_csv(prod_id_details_path, index=False)
    print("PRODUCT COUNT DETAILS SAVE TO:", prod_id_details_path)
    print("\n[7]. TOTAL DEVICE CAPTURE COUNT:", device_id_cnt)
    print("\n[8]. DAILY DEVICE CAPTURE COUNT:", device_id_cnt_daily)
    td_device_details_path = RST_PATH + "td_device_details" + ".csv"
    # pd.DataFrame(rst["td_device"].value_counts()).reset_index().head(10).to_csv(td_device_details_path, index=False)
    print("TOP10 DEVICE COUNT DETAILS SAVE TO:", td_device_details_path)
    print("****OVERALL REPORT END**** \n")


def data_preproc(parm_csv_folder, parm_csv_file_list):
    global CAP_TRX_CNT, TOT_TRX_AMT
    print(os.path.basename(__file__), sys._getframe().f_code.co_name, sys._getframe().f_lineno)
    rst = pd.DataFrame({})

    # gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)
    gc.collect()
    gc.disable()
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_folder, '/', csv_file))
        print('csv_file_path =', csv_file_path)
        csv_file_path = "D:/github_program/myPython/docs/csvfiles/201801/td_device_201801"
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/todo/NO 1_td_1"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding='utf-8', chunksize=5000, iterator=True, sep="|",dtype=str)

        # 3.  数据处理
        df_trx_detail = get_trx_detail(reader)
        print("df_trx_detail =", df_trx_detail.shape)
        CAP_TRX_CNT = CAP_TRX_CNT + df_trx_detail.shape[0]
        TOT_TRX_AMT = TOT_TRX_AMT + df_trx_detail["trx_amount"].sum()
        rst = rst.append(df_trx_detail, ignore_index=True)
        reader.close()
        del reader
        del df_trx_detail
    gc.enable()  # re-enable garbage collection
    gc.collect()
    
    return rst
    """
    print("********************ALL************************")
    sys.exit(0)
    print("--------------------------------------------")
    monitor_TRX_HURDLE = 3000
    monitor_mer_list_by_tot_trx_cnt = \
        mer_with_trx_cnt[mer_with_trx_cnt["trx_cnt"] > monitor_TRX_HURDLE]["mer_id"].tolist()
    print("Monitor Merchant Count :", len(monitor_mer_list_by_tot_trx_cnt))
    print("Monitor Merchant By Total Transaction Count Detail: \n ", monitor_mer_list_by_tot_trx_cnt)
    print("--------------------------------------------")

    print("--------------------------------------------")
    monitor_avg_trx_cnt_by_device_top = 10
    monitor_mer_list_by_avg_trx_cnt = merge_rst_id["mer_id"][0:monitor_avg_trx_cnt_by_device_top].tolist()
    print("Monitor Merchant Count :", len(monitor_mer_list_by_avg_trx_cnt))
    print("Monitor Merchant By Average Transaction Count By Device: \n ", monitor_mer_list_by_avg_trx_cnt)
    print("--------------------------------------------")

    print("--------------------------------------------")
    monitor_mer_by_tot_trx_cnt = pd.DataFrame(
        merge_rst_id[merge_rst_id["mer_id"].isin(monitor_mer_list_by_tot_trx_cnt)])
    print("Total Transaction Count & Average Transaction Count By Device \n"
          "In Monitor Merchant List BY Total Transaction Count: \n", monitor_mer_by_tot_trx_cnt)
    print("final_id.shape:", monitor_mer_by_tot_trx_cnt.shape)
    print("final_id Top10 \n", monitor_mer_by_tot_trx_cnt.head(10))
    print("--------------------------------------------")

    print("--------------------------------------------")
    monitor_mer_by_avg_trx_cnt = pd.DataFrame(
        merge_rst_id[merge_rst_id["mer_id"].isin(monitor_mer_list_by_avg_trx_cnt)])
    print("Total Transaction Count & Average Transaction Count By Device \n"
          "In Monitor Merchant List BY Average Transaction Count: \n", monitor_mer_by_avg_trx_cnt)
    print("--------------------------------------------")

    print("********************************************")
    print("ALL:", rst_id.shape)
    # print(rst_id.head())
    # pvt_id_cnt = pd.pivot_table(rst_id, values=["id_no"], index=["mer_id","td_device"], aggfunc=len).reset_index()
    pvt_id_cnt_new = pd.pivot_table(rst_id,
                                    values=["inst_id"],
                                    index=["mer_id", "td_device", "id_no"],
                                    aggfunc=len) \
        .reset_index()
    # # 4b2c6d5331bbbc05ad9fd70baeb93bd0 5
    # # ec9eb997c715203df55a2c0e6277472a 4
    # monitor_mer_list_id = ["50744","9602"]
    # monitor_mer_list_by_avg_trx_cnt = ["8194"]
    print(len(monitor_mer_list_by_tot_trx_cnt))
    print(monitor_mer_list_by_tot_trx_cnt)
    print(len(monitor_mer_list_by_avg_trx_cnt))
    print(monitor_mer_list_by_avg_trx_cnt)

    tot_monitor_mer_list = monitor_mer_list_by_tot_trx_cnt + monitor_mer_list_by_avg_trx_cnt
    tot_monitor_mer_list = list(set(tot_monitor_mer_list))

    print("monitor_mer_list_by_avg_trx_cnt length:", len(tot_monitor_mer_list))
    tot_susp_di_id_detail = pd.DataFrame({})
    mer_id_list = []
    monitor_id_no_hurdle = 10
    for monitor_mer in tot_monitor_mer_list:
        print("********************************************")
        print("monitor_mer:", monitor_mer)
        monitor_mer_single = pvt_id_cnt_new[pvt_id_cnt_new["mer_id"] == monitor_mer]
        monitor_mer_single_pvt = pd.pivot_table(monitor_mer_single, values=["id_no"], index=["td_device"],
                                                aggfunc=len).reset_index()
        monitor_mer_single_pvt["mer_id"] = monitor_mer
        monitor_mer_single_pvt = monitor_mer_single_pvt[monitor_mer_single_pvt["id_no"] > monitor_id_no_hurdle]
        print("********************************************")
        monitor_mer_single_pvt = monitor_mer_single_pvt.sort_values(by="id_no", axis=0, ascending=False)
        if not monitor_mer_single_pvt.empty:
            mer_id_list.append(monitor_mer)
            td_list = monitor_mer_single_pvt["td_device"].tolist()
            print("Suspicious Device List:", td_list)
            # td_list = ["0761512060b3d16a54a597831e903f34"]
            susp_di_id_detail = pd.DataFrame(pvt_id_cnt_new[pvt_id_cnt_new["td_device"].isin(td_list)])
            tot_susp_di_id_detail = tot_susp_di_id_detail.append(susp_di_id_detail)
    # print("problem mer rst_id:\n",mer_id_list)
    print("********************************************")
    # tot_susp_di_id_detail = tot_susp_di_id_detail.sort_values(by="inst_id", axis=0, ascending=False)
    print("Total Suspicious Device Transaction Detail:\n", tot_susp_di_id_detail.head())
    print(tot_susp_di_id_detail.shape)
    # tot_susp_di_id_detail.to_csv("D:\\github_program\\myPython\\docs\\rst\\final_device_details.csv", index=False)
    print("--------------------------------------------")
    """


def get_csv_folder():
    global csv_biz_type
    father_path = os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep + ".")
    default_dir = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\csvfiles\\'
    csv_folder = "D:/github_program/myPython/docs/csvfiles/todo/"
    # csv_folder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=(os.path.expanduser(default_dir)))
    if len(csv_folder) != 0:
        csv_biz_type = csv_folder.split('/')[-1]
        print('csv_biz_type = ', csv_biz_type)
    else:
        tm.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)

    return csv_folder


def create_rpt(parm_rst):
    if len(parm_rst) != 0:
        print("--------------------------------------------")
        # 1. OVERALL REPORT
        overall_rpt(parm_rst)
        print("--------------------------------------------")
        # 2. SUSPICIOUS DEVICE ANALYSIS
        susp_device_stat(parm_rst)
        print("--------------------------------------------")
        # 3. SUSPICIOUS MERCHANT ANALYSIS
        susp_mer_stat(parm_rst)
    else:
        print("WRONG")
    del parm_rst


def main_process(parm_csv_folder):
    """
        Data Preparation From CSV File & Insert into DB

        Parameters:
          parm_csv_folder: CSV file path

        Returns:
          None

        Raises:
          IOError: An error occurred accessing the bigtable.Table object.
        """
    csv_file_list = os.listdir(parm_csv_folder)
    print('csv_file_list = ', csv_file_list)
    rst = data_preproc(csv_folder, csv_file_list)
    create_rpt(rst)
    print("--------------------------------------------")
    print('Main Processing Have Done...')


def init():
    global RST_PATH, START_DATE, TOT_TRX_AMT, CAP_TRX_CNT, ID_HURDLE, TRX_HURDLE
    father_path = os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep + ".")
    RST_PATH = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\rst\\'
    START_DATE = "2018-01-01"
    TOT_TRX_AMT = 0.
    CAP_TRX_CNT = 0.
    ID_HURDLE = 30
    TRX_HURDLE = 60000
    print("--------------------------------------------")
    print("CONSTANT:")
    print("START_DATE:", START_DATE)
    print("ID_HURDLE:", ID_HURDLE)
    print("TRX_HURDLE:", TRX_HURDLE)
    print("--------------------------------------------")



if __name__ == '__main__':
    init()
    start_time = datetime.datetime.now()
    csv_folder = get_csv_folder()
    print('csv_folder = ', csv_folder)
    main_process(csv_folder)
    end_time = datetime.datetime.now()
    print('start_time =', start_time)
    print('end_time   =', end_time)
    print('diff_Time =', end_time - start_time)
    print('System Processing Done...')

