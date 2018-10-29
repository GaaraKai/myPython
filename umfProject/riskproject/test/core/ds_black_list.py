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
    stat_start_date = "2018-01-01"
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
    print("////////////////////////////////////////////")
    rtn_df["plat_date"] = pd.to_datetime(rtn_df["plat_date"])
    print("stat_start_date = ",stat_start_date)
    print("////////////////////////////////////////////")
    rtn_df = rtn_df[rtn_df["plat_date"] >= stat_start_date]

    return rtn_df


def get_device_report(parm_csv_floder, parm_csv_file_list):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name, sys._getframe().f_lineno)
    father_path = os.path.abspath(os.path.dirname(parm_csv_floder) + os.path.sep + ".")
    rst_dir = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\rst\\'
    rst = pd.DataFrame({})
    td_device_rst = pd.DataFrame({})
    cap_trx_cnt = 0
    total_trx_amount = .0
    date_range = 31
    total_intercept_cnt = 0
    total_intercept_amt = 0
    daily_intercept_cnt = 0
    daily_intercept_amt = 0
    # gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)
    gc.collect()
    gc.disable()
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_floder, '/', csv_file))
        print('csv_file_path =', csv_file_path)
        csv_file_path = "D:/github_program/myPython/docs/csvfiles/201801/td_device_201801"
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
    if len(rst) != 0:
        print("////////////////////////////////////////////")
        print(rst.shape)
        print("////////////////////////////////////////////")
        print("Historical Data:")
        total_trx_amount = round(total_trx_amount / 100000000, 2)
        print("Total Device ID Transaction Count:", cap_trx_cnt)
        print("Total Device ID Transaction Amount:", total_trx_amount)
        # mer_id_cnt = np.array(rst["mer_id"].unique()).size
        mer_id_cnt = np.array(rst["mer_id"].unique())
        prod_id_cnt = pd.DataFrame(np.array(rst["prod_id"].unique()).tolist()).dropna(axis=0, how='any').size
        device_id_cnt = pd.DataFrame(np.array(rst["td_device"].unique()).tolist()).dropna(axis=0, how='any').size
        device_id_cnt_daily = round(device_id_cnt / date_range, 2)
        print("Total Device ID Merchant Count:", mer_id_cnt.size)
        # print("Total Device ID Merchant ID:", mer_id_cnt)
        print("Total Device ID Product Count:", prod_id_cnt)
        print("Total Device ID Capture Count:", device_id_cnt)
        print("Daily Total Device ID Capture Count:", device_id_cnt_daily)
        print("////////////////////////////////////////////")

        print("////////////////////////////////////////////")
        prod_id_details_path = rst_dir + "prod_id_details" + ".csv"
        # pd.DataFrame(rst["prod_id"].value_counts()).reset_index().to_csv(prod_id_details_path, index=False)
        print("1.ALL Product ID:", prod_id_details_path)
        print("////////////////////////////////////////////")

        print("////////////////////////////////////////////")
        mer_id_details_path = rst_dir + "mer_id_details" + ".csv"
        # pd.DataFrame(rst["mer_id"].value_counts()).reset_index().head(10).to_csv(mer_id_details_path, index=False)
        print("2.TOP10 Merchant ID:", mer_id_details_path)
        print("////////////////////////////////////////////")

        print("////////////////////////////////////////////")
        td_device_details_path = rst_dir + "td_device_details" + ".csv"
        # pd.DataFrame(rst["td_device"].value_counts()).reset_index().head(10).to_csv(td_device_details_path, index=False)
        print("3.TOP10 Device ID:", td_device_details_path)
        print("////////////////////////////////////////////")
        # print(rst.shape)  # (424699, 16)
        # mobile = rst.dropna(axis=0, subset=["mobile_no"])
        # mobile = mobile[mobile["mobile_no"] != "00000000000"]
        # print(mobile.shape)  # (283686, 16)

        rst_id = rst.dropna(axis=0, subset=["id_no"])
        print(rst_id.shape)  # (274038, 16)

        print("////////////////////////////////////////////")
        # print(rst.shape)
        # dup_mer = rst.drop_duplicates(["mer_id"])
        # dup_mer_a = dup_mer["mer_id"]
        print("////////////////////////////////////////////")
        print("Transaction Count by one TD device")
        """
        print("********************************************")
        print("ALL:", rst.shape)
        no_dup_cnt = pd.pivot_table(rst, index=["mer_id"], values=["td_device"], aggfunc=len)
        no_dup_cnt = no_dup_cnt.sort_values(by='td_device', axis=0, ascending=False).reset_index()
        print(no_dup_cnt)

        monitor_mer_list = no_dup_cnt[no_dup_cnt["td_device"] > 3000]["mer_id"].tolist()
        print("monitor_mer count:",len(monitor_mer_list))   # 29
        print("monitor_mer_list \n", monitor_mer_list)
        print("////////////////////////////////////////////")

        print("////////////////////////////////////////////")
        dup = rst.drop_duplicates(["mer_id","td_device"])
        print("drop_duplicates([mer_id+td_device]: ",dup.shape)
        dup_cnt = pd.pivot_table(dup, index=["mer_id"], values=["td_device"], aggfunc=len)
        # xx = xx.sort_values(by='td_device', axis=0, ascending=False).reset_index()
        dup_cnt = dup_cnt.sort_values(by='td_device', axis=0, ascending=False).reset_index()
        # dup_cnt = dup_cnt[dup_cnt["td_device"] < 300]
        print("////////////////////////////////////////////")

        merge_rst = pd.merge(no_dup_cnt,dup_cnt , how='inner', on=['mer_id'],suffixes=("_ori","_dup"))
        merge_rst["avg_trx_cnt_by_one_device"] = round(merge_rst["td_device_ori"] / merge_rst["td_device_dup"], 0)
        merge_rst = merge_rst.sort_values(by='avg_trx_cnt_by_one_device', axis=0, ascending=False)
        print("////////////////////////////////////////////")
        # print(merge_rst["mer_id"])
        # print(monitor_mer_list)
        print("////////////////////////////////////////////")
        final = pd.DataFrame(merge_rst[merge_rst["mer_id"].isin(monitor_mer_list)])
        # final = final[final["tot_trx_cnt_by_one_device"] > 5]
        print("final.shape:",final.shape)
        print("final Top10 \n", final.head(10))
        """

        print("********************ALL************************")
        mark = "ALL TRANSACTION"
        print("ALL TRANSACTION:", rst.shape)




        all_mer_with_trx_cnt = pd.pivot_table(rst, index=["mer_id"], values=["inst_id"], aggfunc=len) \
            .sort_values(by='inst_id', axis=0, ascending=False).reset_index()
        all_mer_with_trx_cnt.rename(columns={"inst_id": "trx_cnt"}, inplace=True)
        print("Top10 Merchant of Count Transaction: \n", all_mer_with_trx_cnt.head(10))



        print("////////////////////////////////////////////")
        rst_drop_mer_td = rst.drop_duplicates(["mer_id", "td_device"])
        print("rst_drop_id_mer_td:", rst_drop_mer_td.shape)

        # mer_with_td_device_cnt:去重后，一个商户对应的设备指纹个数
        all_mer_with_td_device_cnt = pd.pivot_table(rst_drop_mer_td, index=["mer_id"], values=["td_device"], aggfunc=len) \
            .sort_values(by='td_device', axis=0, ascending=False).reset_index()
        all_mer_with_td_device_cnt.rename(columns={"td_device": "device_cnt"}, inplace=True)
        print("////////////////////////////////////////////")
        all_merge_rst = pd.merge(all_mer_with_trx_cnt, all_mer_with_td_device_cnt, how='inner', on=['mer_id'])
        all_merge_rst["avg_trx_cnt_by_one_device_id"] = round(all_merge_rst["trx_cnt"] / all_merge_rst["device_cnt"], 0)
        all_merge_rst = all_merge_rst.sort_values(by='avg_trx_cnt_by_one_device_id', axis=0, ascending=False)
        print("Total Transaction Count & Average Transaction Count By Device(TOP5): \n", all_merge_rst.head(5))
        print(all_merge_rst.shape)
        sys.exit(0)
        print("////////////////////////////////////////////")

        print("////////////////////////////////////////////")
        monitor_trx_cnt_hurdle = 3000
        all_monitor_mer_list_by_tot_trx_cnt = \
            all_mer_with_trx_cnt[all_mer_with_trx_cnt["trx_cnt"] > monitor_trx_cnt_hurdle]["mer_id"].tolist()
        print("Monitor Merchant Count :", len(all_monitor_mer_list_by_tot_trx_cnt))
        print("Monitor Merchant By Total Transaction Count Detail: \n ", all_monitor_mer_list_by_tot_trx_cnt)
        print("////////////////////////////////////////////")

        print("////////////////////////////////////////////")
        monitor_avg_trx_cnt_by_device_top = 10
        all_monitor_mer_list_by_avg_trx_cnt = all_merge_rst["mer_id"][0:monitor_avg_trx_cnt_by_device_top].tolist()
        print("Monitor Merchant Count :", len(all_monitor_mer_list_by_avg_trx_cnt))
        print("Monitor Merchant By Average Transaction Count By Device: \n ", all_monitor_mer_list_by_avg_trx_cnt)
        print("////////////////////////////////////////////")

        print("////////////////////////////////////////////")
        all_monitor_mer_by_tot_trx_cnt = pd.DataFrame(
            all_merge_rst[all_merge_rst["mer_id"].isin(all_monitor_mer_list_by_tot_trx_cnt)])
        print("Total Transaction Count & Average Transaction Count By Device "
              "In Monitor Merchant List BY Total Transaction Count: \n", all_monitor_mer_by_tot_trx_cnt)
        print("final_id.shape:", all_monitor_mer_by_tot_trx_cnt.shape)
        print("final_id Top10 \n", all_monitor_mer_by_tot_trx_cnt.head(10))
        print("////////////////////////////////////////////")

        print("////////////////////////////////////////////")
        all_monitor_mer_by_avg_trx_cnt = pd.DataFrame(
            all_merge_rst[all_merge_rst["mer_id"].isin(all_monitor_mer_list_by_avg_trx_cnt)])
        print("Total Transaction Count & Average Transaction Count By Device "
              "In Monitor Merchant List BY Average Transaction Count: \n", all_monitor_mer_by_avg_trx_cnt)
        print("////////////////////////////////////////////")

        print("********************************************")
        print("ALL:", rst.shape)
        # print(rst_id.head())
        # pvt_id_cnt = pd.pivot_table(rst_id, values=["id_no"], index=["mer_id","td_device"], aggfunc=len).reset_index()
        pvt_id_cnt_new = pd.pivot_table(rst,
                                        values=["inst_id"],
                                        index=["mer_id", "td_device","inst_trace"],
                                        aggfunc=len) \
            .reset_index()
        # print(pvt_id_cnt_new[pvt_id_cnt_new["mer_id"]== "3959046"])
        # sys.exit(0)
        print(len(all_monitor_mer_list_by_tot_trx_cnt))
        print(all_monitor_mer_list_by_tot_trx_cnt)
        print(len(all_monitor_mer_list_by_avg_trx_cnt))
        print(all_monitor_mer_list_by_avg_trx_cnt)
        all_tot_monitor_mer_list = all_monitor_mer_list_by_tot_trx_cnt + all_monitor_mer_list_by_avg_trx_cnt
        all_tot_monitor_mer_list = list(set(all_tot_monitor_mer_list))
        print("ALL ","monitor_mer_list_by_avg_trx_cnt length:", len(all_tot_monitor_mer_list))
        tot_susp_di_id_detail = pd.DataFrame({})
        mer_id_list = []
        monitor_id_no_hurdle = 100
        # all_tot_monitor_mer_list = ["3959046"]

        for monitor_mer in all_tot_monitor_mer_list:
            print("********************************************")
            print("monitor_mer:", monitor_mer)
            # print(pvt_id_cnt_new["mer_id"].unique())
            monitor_mer_single = pvt_id_cnt_new[pvt_id_cnt_new["mer_id"] == monitor_mer]
            monitor_mer_single_pvt = pd.pivot_table(monitor_mer_single, values=["inst_id"], index=["td_device"],
                                                    aggfunc=len).reset_index()
            monitor_mer_single_pvt["mer_id"] = monitor_mer

            # print(monitor_mer_single_pvt)
            monitor_mer_single_pvt = monitor_mer_single_pvt[monitor_mer_single_pvt["inst_id"] > monitor_id_no_hurdle]

            print("********************************************")
            monitor_mer_single_pvt = monitor_mer_single_pvt.sort_values(by="inst_id", axis=0, ascending=False)
            if not monitor_mer_single_pvt.empty:
                mer_id_list.append(monitor_mer)
                td_list = monitor_mer_single_pvt["td_device"].tolist()
                print("Suspicious Device List:", td_list)
                susp_di_id_detail = pd.DataFrame(pvt_id_cnt_new[pvt_id_cnt_new["td_device"].isin(td_list)])
                tot_susp_di_id_detail = tot_susp_di_id_detail.append(susp_di_id_detail)
        print("********************************************")
        print("Total Suspicious Device Transaction Detail:\n", tot_susp_di_id_detail)
        print(tot_susp_di_id_detail.shape)

        print("********************ALL finish************************")
        # sys.exit(0)
        print("********************final************************")
        print("ALL:", rst_id.shape)
        mer_with_trx_cnt = pd.pivot_table(rst_id, index=["mer_id"], values=["td_device"], aggfunc=len) \
            .sort_values(by='td_device', axis=0, ascending=False).reset_index()
        mer_with_trx_cnt.rename(columns={"td_device": "trx_cnt"}, inplace=True)
        print("Top10 Merchant of Count Transaction: \n", mer_with_trx_cnt.head(10))

        print("////////////////////////////////////////////")
        rst_drop_id_mer_td = rst_id.drop_duplicates(["mer_id", "td_device"])
        print("rst_drop_id_mer_td:", rst_drop_id_mer_td.shape)

        # mer_with_td_device_cnt:去重后，一个商户对应的设备指纹个数
        mer_with_td_device_cnt = pd.pivot_table(rst_drop_id_mer_td, index=["mer_id"], values=["td_device"], aggfunc=len) \
            .sort_values(by='td_device', axis=0, ascending=False).reset_index()
        mer_with_td_device_cnt.rename(columns={"td_device": "device_cnt"}, inplace=True)
        print("////////////////////////////////////////////")
        merge_rst_id = pd.merge(mer_with_trx_cnt, mer_with_td_device_cnt, how='inner', on=['mer_id'])
        merge_rst_id["avg_trx_cnt_by_one_device_id"] = round(merge_rst_id["trx_cnt"] / merge_rst_id["device_cnt"], 0)
        merge_rst_id = merge_rst_id.sort_values(by='avg_trx_cnt_by_one_device_id', axis=0, ascending=False)
        print("Total Transaction Count & Average Transaction Count By Device: \n", merge_rst_id.head(5))
        print("////////////////////////////////////////////")


        print("////////////////////////////////////////////")
        monitor_trx_cnt_hurdle = 3000
        monitor_mer_list_by_tot_trx_cnt = \
            mer_with_trx_cnt[mer_with_trx_cnt["trx_cnt"] > monitor_trx_cnt_hurdle]["mer_id"].tolist()
        print("Monitor Merchant Count :", len(monitor_mer_list_by_tot_trx_cnt))
        print("Monitor Merchant By Total Transaction Count Detail: \n ", monitor_mer_list_by_tot_trx_cnt)
        print("////////////////////////////////////////////")

        print("////////////////////////////////////////////")
        monitor_avg_trx_cnt_by_device_top = 10
        monitor_mer_list_by_avg_trx_cnt = merge_rst_id["mer_id"][0:monitor_avg_trx_cnt_by_device_top].tolist()
        print("Monitor Merchant Count :", len(monitor_mer_list_by_avg_trx_cnt))
        print("Monitor Merchant By Average Transaction Count By Device: \n ", monitor_mer_list_by_avg_trx_cnt)
        print("////////////////////////////////////////////")

        print("////////////////////////////////////////////")
        monitor_mer_by_tot_trx_cnt = pd.DataFrame(
            merge_rst_id[merge_rst_id["mer_id"].isin(monitor_mer_list_by_tot_trx_cnt)])
        print("Total Transaction Count & Average Transaction Count By Device \n"
              "In Monitor Merchant List BY Total Transaction Count: \n", monitor_mer_by_tot_trx_cnt)
        print("final_id.shape:", monitor_mer_by_tot_trx_cnt.shape)
        print("final_id Top10 \n", monitor_mer_by_tot_trx_cnt.head(10))
        print("////////////////////////////////////////////")

        print("////////////////////////////////////////////")
        monitor_mer_by_avg_trx_cnt = pd.DataFrame(
            merge_rst_id[merge_rst_id["mer_id"].isin(monitor_mer_list_by_avg_trx_cnt)])
        print("Total Transaction Count & Average Transaction Count By Device \n"
              "In Monitor Merchant List BY Average Transaction Count: \n", monitor_mer_by_avg_trx_cnt)
        print("////////////////////////////////////////////")

        print("********************************************")
        print("ALL:", rst_id.shape)
        # print(rst_id.head())
        # pvt_id_cnt = pd.pivot_table(rst_id, values=["id_no"], index=["mer_id","td_device"], aggfunc=len).reset_index()
        pvt_id_cnt_new = pd.pivot_table(rst_id,
                                        values=["inst_id"],
                                        index=["mer_id", "td_device", "id_no"],
                                        aggfunc=len) \
            .reset_index()

        # print(pvt_id_cnt_new)

        # xx = pvt_id_cnt_new[pvt_id_cnt_new["mer_id"] == "50744"]
        # yy = pd.pivot_table(xx, values=["id_no"], index=["td_device"], aggfunc=len).reset_index()
        # yy = yy[yy["id_no"] > 3]
        # print(yy)
        # # 4b2c6d5331bbbc05ad9fd70baeb93bd0 5
        # # ec9eb997c715203df55a2c0e6277472a 4
        # aa = pvt_id_cnt_new[pvt_id_cnt_new["td_device"] == "c0eecdfdb49183b364ceab2c928375cf"]
        # print(aa)
        #
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


        sys.exit(0)

        yy = dup_cnt.describe()
        print(yy)
        plt.bar(dup_cnt.index, dup_cnt["td_device"], facecolor='red', width=3, label='CAP_CNT')
        plt.legend()
        plt.show()
        print("////////////////////////////////////////////")
        print("////////////////////////////////////////////")
    else:
        td_device_piv = pd.DataFrame({})
    print("////////////////////////////////////////////")



    #
    # print("////////////////////////////////////////////")
    # print("The Count of In the TD_device GRAY List & exist transaction in historical data:",
    #       td_device_rst.shape[0])
    # print("Based on historical data, the NEGATIVE Effects if import TD_device GRAY List is:")
    # print("Total Count of Gray List interception will be increase:", total_intercept_cnt)
    # print("Total Amount of Gray List interception will be increase:", total_intercept_amt)
    # print("Daily Total Count of Gray List interception will be increase:", daily_intercept_cnt)
    # print("Daily Total Amount of Gray List interception will be increase:", daily_intercept_amt)
    # print("TOP10 TD_device:\n", td_device_rst.head(10))
    # print("////////////////////////////////////////////")
    del rst
    del td_device_rst
    return 0

def get_csv_floder():
    global csv_biz_type
    father_path = os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep + ".")
    default_dir = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\csvfiles\\'
    csv_floder = "D:/github_program/myPython/docs/csvfiles/todo/"
    # csv_floder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=(os.path.expanduser(default_dir)))
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



