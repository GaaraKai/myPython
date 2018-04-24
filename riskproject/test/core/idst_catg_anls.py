# -*- coding: utf-8 -*-
import datetime
import gc
import logging
import os
import sys
import time
import tkinter.filedialog as tf
import tkinter.messagebox as tm
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


import numpy as np
import pandas as pd

from riskproject.test.core import config


class Logger:
    def __init__(self, path, slevel=logging.DEBUG, flevel=logging.DEBUG):
        self.logger = logging.getLogger(path)
        self.logger.setLevel(logging.DEBUG)
        # fmt = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
        fmt = logging.Formatter("")
        # LOG on screen
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        sh.setLevel(slevel)
        # LOG in file
        fh = logging.FileHandler(path)
        fh.setFormatter(fmt)
        fh.setLevel(flevel)
        self.logger.addHandler(sh)
        self.logger.addHandler(fh)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def war(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def cri(self, message):
        self.logger.critical(message)


def get_trx_detail(parm_reader):
    # global START_DATE
    rtn_df = pd.DataFrame({})
    for chunk in parm_reader:
        rtn_df = rtn_df.append(chunk)
    rtn_df.drop(["INSTID"], axis=1, inplace=True)
    rtn_df.drop(["TRADENO"], axis=1, inplace=True)
    rtn_df.drop(["ORDERID"], axis=1, inplace=True)
    rtn_df.drop(["MERID"], axis=1, inplace=True)
    rtn_df.drop(["USERMERIP"], axis=1, inplace=True)
    rtn_df.drop(["USERUMPIP"], axis=1, inplace=True)
    rtn_df.drop(["TD_DEVICEID"], axis=1, inplace=True)
    rtn_df.drop(["ZY_DEVICEID"], axis=1, inplace=True)
    rtn_df.drop(["IDENTIFYCODE"], axis=1, inplace=True)
    # rtn_df.drop(["PAYSTATE"], axis=1, inplace=True)
    rtn_df.drop(["MOBILEID"], axis=1, inplace=True)
    rtn_df.drop(["PRODUCTID"], axis=1, inplace=True)
    rtn_df.drop(["CARDHOLDER"], axis=1, inplace=True)
    rtn_df.drop(["BACK_UP3"], axis=1, inplace=True)
    rtn_df.drop(["UPDATE_TIME"], axis=1, inplace=True)
    rtn_df.drop(["Unnamed: 21"], axis=1, inplace=True)
    rtn_df.rename(columns={
          # "INSTID": "inst_id"
         "TRACE": "inst_trace"
        # , "TRADENO": "inner_trade_id"
        # , "ORDERID": "order_id"
        # , "MERID": "mer_id"
        # , "USERMERIP": "order_ip"
        # , "USERUMPIP": "pay_ip"
        # , "TD_DEVICEID": "td_device"
        # , "ZY_DEVICEID": "zy_device"
        # , "IDENTIFYCODE": "id_no"
        , "AMOUNT": "trx_amount"
        , "PLATDATE": "plat_date"
        , "PLATTIME": "plat_time"
        , "PAYSTATE": "pay_status"
        # , "MOBILEID": "mobile_no"
        # , "PRODUCTID": "  prod_id"
        # , "CARDHOLDER": "card_holder"
        , "MERCH_INDUSTRY": "mer_idst"
        , "RISK_LEVEL": "risk_lvl"
        # , "BACK_UP3": "sale_nm"
        # , "UPDATE_TIME": "update_time"
        }, inplace=True)
    # logger.info(rtn_df.dtypes)
    rtn_df["plat_date"] = pd.to_datetime(rtn_df["plat_date"])
    rtn_df = rtn_df[(rtn_df["plat_date"] >= conf.START_DATE) & (rtn_df["plat_date"] <= conf.END_DATE)]
    # logger.info(rtn_df)
    rtn_df = rtn_df[rtn_df["pay_status"] == "0"]
    # rtn_df = rtn_df[rtn_df["mer_idst"] == "4"]
    rtn_df["trx_amount"] = rtn_df["trx_amount"].astype("float64")
    rtn_df["trx_amount"] = rtn_df["trx_amount"]*1000
    return rtn_df


def susp_device_stat(parm_df):
    logger.info("****SUSPICIOUS DEVICE ANALYSIS BEGIN****")
    logger.info("[1]. ALL TRANSACTION WITH DROP NULL ID_NO[SHAPE]:")
    rst_dropna_id_no = parm_df.dropna(axis=0, subset=["id_no"])
    logger.info(rst_dropna_id_no.shape)
    logger.info("[2]. AFTER [1], DROP DUPLICATED TD_DEVICE & ID_NO[SHAPE]:")
    rst_dropna_id_no_dup = rst_dropna_id_no.drop_duplicates(["td_device", "id_no"])
    logger.info(rst_dropna_id_no_dup.shape)
    logger.info("[3]. FIND COUNT OF ID_NO BY EVERY DEVICE[TOP5]:")
    devi_with_id_cnt = pd.pivot_table(rst_dropna_id_no_dup, index=["td_device"], values=["id_no"], aggfunc=len) \
        .sort_values(by='id_no', axis=0, ascending=False).reset_index()
    devi_with_id_cnt.rename(columns={"id_no": "id_no_cnt"}, inplace=True)
    logger.info(devi_with_id_cnt.head())
    logger.info("[4]. FIND SUSPICIOUS DEVICE WHERE ID HURDLE IS[%s]:" % conf.ID_HURDLE)
    susp_device_list = pd.Series(devi_with_id_cnt[devi_with_id_cnt["id_no_cnt"] >= conf.ID_HURDLE]["td_device"]) \
        .tolist()
    if len(susp_device_list) == 0:
        logger.info("NO SUSPICIOUS DEVICE BECAUSE ID HURDLE %s IS TOO HIGH." % conf.ID_HURDLE)
    else:
        logger.info("==>> SUSPICIOUS DEVICE: %s" % susp_device_list)
        logger.info("[5]. FIND MERCHANT WHIT SUSPICIOUS DEVICE:")
        mer_with_susp_device = pd.DataFrame(
            rst_dropna_id_no_dup[rst_dropna_id_no_dup["td_device"].isin(susp_device_list)])
        mer_with_susp_device_list = mer_with_susp_device["mer_id"].unique().tolist()
        logger.info("==>> SUSPICIOUS MERCHANT: %s" % mer_with_susp_device_list)
        mer_with_susp_device_detl = pd.pivot_table(mer_with_susp_device,
                                                   values=["inst_id"],
                                                   index=["mer_id", "td_device", "id_no"],
                                                   aggfunc=len).sort_values(by='inst_id', axis=0,
                                                                            ascending=False).reset_index()
        logger.info("[6]. FIND SUSPICIOUS DEVICE TRANSACTION DETAILS:")
        logger.info("==>> SUSPICIOUS TRANSACTION[e.g.]:")
        logger.info(mer_with_susp_device_detl.head())
        logger.info("[7]. SAVE SUSPICIOUS DEVICE TRANSACTION DETAILS:")
        mer_with_susp_device_trx_details_path = conf.RESULT_PATH + "mer_with_susp_device_trx_details" + ".csv"
        # mer_with_susp_device.to_csv(mer_with_susp_device_trx_details_path, index=False)
        logger.info("SAVE TO: %s" % mer_with_susp_device_trx_details_path)
    logger.info("****SUSPICIOUS DEVICE ANALYSIS END****")
    del parm_df


def susp_mer_stat(parm_rst):
    # global RST_PATH, TRX_HURDLE
    logger.info("****SUSPICIOUS MERCHANT ANALYSIS BEGIN****")
    logger.info("[1]. FIND MERCHANT TRANSACTION COUNT[TOP5]:")
    mer_with_trx_cnt = pd.pivot_table(parm_rst, index=["mer_id"], values=["inst_id"], aggfunc=len) \
        .sort_values(by='inst_id', axis=0, ascending=False).reset_index()
    mer_with_trx_cnt.rename(columns={"inst_id": "trx_cnt"}, inplace=True)
    logger.info(mer_with_trx_cnt.head())

    logger.info("[2]. FIND MERCHANT DEVICE COUNT[TOP5]:")
    rst_drop_mer_td = parm_rst.drop_duplicates(["mer_id", "td_device"])
    mer_with_td_device_cnt = pd.pivot_table(rst_drop_mer_td, index=["mer_id"], values=["td_device"],
                                            aggfunc=len) \
        .sort_values(by='td_device', axis=0, ascending=False).reset_index()
    mer_with_td_device_cnt.rename(columns={"td_device": "device_cnt"}, inplace=True)
    logger.info(mer_with_td_device_cnt.head())
    logger.info("[3]. MERGE TRANSACTION & DEVICE COUNT BY MERCHANT ID[TOP5]:")
    merge_rst = pd.merge(mer_with_trx_cnt, mer_with_td_device_cnt, how='inner', on=['mer_id'])
    merge_rst["avg_trx_cnt_by_one_device"] = \
        round(merge_rst["trx_cnt"] / merge_rst["device_cnt"], 0)
    logger.info(merge_rst.head())
    logger.info("SHAPE:")
    logger.info(merge_rst.shape)
    logger.info("[4]. SAVE MERCHANT TRANSACTION & DEVICE COUNT STATISTICAL TABLE:")
    mer_with_susp_device_trx_details_path = conf.RESULT_PATH + "mer_with_susp_device_trx_details" + ".csv"
    # mer_with_susp_device.to_csv(mer_with_susp_device_trx_details_path, index=False)
    logger.info("SAVE TO: %s" % mer_with_susp_device_trx_details_path)
    logger.info("[5] FIND MERCHANT WHERE TRANSACTION COUNT OVER HURDLE[%s]:" % conf.TRX_HURDLE)
    key_mer_by_trx_list = pd.Series(
        mer_with_trx_cnt[mer_with_trx_cnt["trx_cnt"] >= conf.TRX_HURDLE]["mer_id"]).tolist()
    if len(key_mer_by_trx_list) == 0:
        logger.info("==>> NO MERCHANT BECAUSE TRX_HURDLE [%s] IS TOO HIGH." % conf.TRX_HURDLE)
    else:
        logger.info("==>> MONITOR MERCHANT COUNT: %s" % len(key_mer_by_trx_list))
        logger.info("==>> MONITOR MERCHANT DETAILS: %s" % key_mer_by_trx_list)
        logger.info("==>> MONITOR MERCHANT TRANSACTION COUNT[OVER HURDLE]:")
        key_mer_trx_stat = pd.DataFrame(merge_rst[merge_rst["mer_id"].isin(key_mer_by_trx_list)])
        logger.info(key_mer_trx_stat)
    avg_trx_top = 5
    logger.info("[6] FIND MERCHANT WHERE AVERAGE TRANSACTION COUNT WITH DEVICE[TOP %s]:" % avg_trx_top)
    key_mer_avg_stat = pd.DataFrame(
        merge_rst.sort_values(by='avg_trx_cnt_by_one_device', axis=0, ascending=False))[0:avg_trx_top]
    key_mer_by_avg_list = pd.Series(key_mer_avg_stat["mer_id"]).tolist()
    logger.info("==>> SUSPICIOUS MERCHANT COUNT: %s" % (len(key_mer_by_avg_list)))
    logger.info("==>> SUSPICIOUS MERCHANT ID: %s" % key_mer_by_avg_list)
    logger.info("==>> SUSPICIOUS MERCHANT TRANSACTION COUNT[TOP5]:")
    logger.info(key_mer_avg_stat)
    logger.info("****SUSPICIOUS MERCHANT ANALYSIS END****")
    del parm_rst


def mer_idst_stat(parm_rst):
    logger.info("****MERCHANT INDUSTRY ANALYSIS BEGIN****")
    logger.info("[1]. REPLACE MERCHANT INDUSTRY VALUE:")
    parm_rst["mer_idst"].replace(idst_conf.mer_idst_dict,inplace=True)
    logger.info("==>> FINISHED...")

    logger.info("[2]. FIND MERCHANT INDUSTRY LIST:")
    mer_idst_list = pd.Series(parm_rst["mer_idst"].unique()).tolist()
    if len(mer_idst_list) == 0:
        logger.info("NO INDUSTRY INFORMATION, PLEASE CHECK AGAIN.")
    else:
        logger.info("==>> MERCHANT INDUSTRY LIST COUNT: %s" % len(mer_idst_list))
        logger.info("==>> MERCHANT INDUSTRY LIST DETAILS: %s" % mer_idst_list)
        logger.info("[3]. FIND TRANSACTION COUNT BY INDUSTRY[TOP5]:")
        mer_with_trx_cnt = pd.pivot_table(parm_rst, index=["mer_idst"], values=["inst_trace"], aggfunc=len) \
            .sort_values(by='inst_trace', axis=0, ascending=False).reset_index()
        mer_with_trx_cnt.rename(columns={"inst_trace": "trx_cnt"}, inplace=True)
        logger.info(mer_with_trx_cnt.head())

        logger.info("[4]. FIND TRANSACTION AMOUNT BY INDUSTRY[TOP5]:")
        mer_with_trx_amt = pd.pivot_table(parm_rst, index=["mer_idst"], values=["trx_amount"],
                                                aggfunc=np.sum) \
            .sort_values(by='trx_amount', axis=0, ascending=False).reset_index()
        mer_with_trx_amt["trx_amount"] = round(mer_with_trx_amt["trx_amount"],2)
        logger.info(mer_with_trx_amt.head())

        logger.info("[5]. MERGE TRANSACTION COUNT & AMOUNT BY INDUSTRY[TOP5]:")
        merge_rst = pd.merge(mer_with_trx_cnt, mer_with_trx_amt, how='inner', on=['mer_idst'])
        logger.info(merge_rst)
        # mer_idst_list = ["6"]
        # logger.info(mer_idst_list)

        logger.info("[6]. SAVE SINGLE INDUSTRY ANALYSIS RESULT:")
        all_daily_trx_cnt = pd.DataFrame({})
        for single_idst in mer_idst_list:
            single_idst_df = parm_rst[parm_rst["mer_idst"] == single_idst]
            daily_trx_cnt = pd.pivot_table(single_idst_df, index=["plat_date"], values=["trx_amount"],
                                           aggfunc=[len,np.sum]).reset_index()
            daily_trx_cnt.columns = ["plat_date","trx_cnt", "trx_amt"]
            daily_trx_cnt["trx_cnt"] = round(daily_trx_cnt["trx_cnt"],0)
            daily_trx_cnt["trx_amt"] = round(daily_trx_cnt["trx_amt"]/10000,2)
            daily_trx_cnt["mer_idst"] = single_idst
            logger.info("==>> INDUSTRY: %s, COUNT: %s" % (single_idst,len(daily_trx_cnt)))
            all_daily_trx_cnt = all_daily_trx_cnt.append(daily_trx_cnt)
            del daily_trx_cnt
        all_daily_trx_cnt.sort_values(by='mer_idst', axis=0, ascending=False).reset_index()
        # logger.info(all_daily_trx_cnt)
        idst_daily_trx_cnt_path = conf.RESULT_PATH + "indu_catg_anls/all_daily_trx_cnt.csv"
        logger.info("SAVE TO: %s" % idst_daily_trx_cnt_path)
        all_daily_trx_cnt.to_csv(idst_daily_trx_cnt_path, index=False)
    logger.info("****MERCHANT INDUSTRY ANALYSIS  END****")
    del parm_rst


def overall_rpt(parm_rst):
    global CAP_TRX_CNT, TOT_TRX_AMT
    logger.info("****OVERALL REPORT START****")
    logger.info("[1]. TOTAL DATA SHAPE:")
    logger.info(parm_rst.shape)
    logger.info("[2]. STATISTICS RANGE: FROM %s TO %s" % (conf.START_DATE, conf.END_DATE))
    total_trx_amount = round(TOT_TRX_AMT / 100000, 2)
    logger.info("[3]. TOTAL TRANSACTION COUNT: %s" % CAP_TRX_CNT)
    logger.info("[4]. TOTAL TRANSACTION AMOUNT: %s BILLION" % total_trx_amount)
    # mer_id_cnt = np.array(parm_rst["mer_id"].unique())
    mer_idst_cnt = pd.DataFrame(np.array(parm_rst["mer_idst"].unique()).tolist()).dropna(axis=0, how='any').size
    risk_lvl_cnt = pd.DataFrame(np.array(parm_rst["risk_lvl"].unique()).tolist()).dropna(axis=0, how='any').size
    # device_id_cnt_daily = round(device_id_cnt / conf.DATE_RANGE, 2)
    # logger.info("[5]. TOTAL MERCHANT COUNT: %s" % mer_id_cnt.size)
    mer_id_details_path = conf.RESULT_PATH + "mer_id_details" + ".csv"
    # pd.DataFrame(rst["mer_id"].value_counts()).reset_index().head(10).to_csv(mer_id_details_path, index=False)
    logger.info("TOP10 MERCHANT COUNT DETAILS SAVE TO %s" % mer_id_details_path)
    logger.info("[6]. TOTAL mer_idst_cnt: %s" % mer_idst_cnt)
    prod_id_details_path = conf.RESULT_PATH + "prod_id_details" + ".csv"
    # pd.DataFrame(rst["prod_id"].value_counts()).reset_index().to_csv(prod_id_details_path, index=False)
    logger.info("PRODUCT COUNT DETAILS SAVE TO: %s" % prod_id_details_path)
    logger.info("[7]. TOTAL risk_lvl_cntT: %s" % risk_lvl_cnt)
    # logger.info("[8]. DAILY DEVICE CAPTURE COUNT: %s" % device_id_cnt_daily)
    td_device_details_path = conf.RESULT_PATH + "td_device_details" + ".csv"
    # pd.DataFrame(rst["td_device"].value_counts()).reset_index().head(10).to_csv(td_device_details_path, index=False)
    logger.info("TOP10 DEVICE COUNT DETAILS SAVE TO %s" % td_device_details_path)
    logger.info("****OVERALL REPORT END****")


def data_preproc(parm_csv_folder, parm_csv_file_list):
    global CAP_TRX_CNT, TOT_TRX_AMT
    CAP_TRX_CNT = 0.
    TOT_TRX_AMT = 0.
    rst = pd.DataFrame({})

    # gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)
    gc.collect()
    gc.disable()
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_folder, '/', csv_file))
        logger.info("csv_file_path = %s" % csv_file_path)
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/indu_catg_anls/NO 1_1_1"
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/indu_catg_anls/test.txt"
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/todo_td/NO 1_td_1"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding='utf-8', chunksize=5000, iterator=True, sep="|", dtype=str)

        # 3.  数据处理
        df_trx_detail = get_trx_detail(reader)
        logger.info("SINGLE CHUNK SHAPE:")
        logger.info(df_trx_detail.shape)
        CAP_TRX_CNT = CAP_TRX_CNT + df_trx_detail.shape[0]
        TOT_TRX_AMT = TOT_TRX_AMT + df_trx_detail["trx_amount"].sum()
        rst = rst.append(df_trx_detail, ignore_index=True)
        reader.close()
        del reader
        del df_trx_detail
    gc.enable()  # re-enable garbage collection
    gc.collect()

    return rst


def get_csv_folder():
    # father_path = os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep + ".")
    # default_dir = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\csvfiles'
    # csv_folder = "D:/github_program/myPython/docs/csvfiles/todo_td/"
    csv_folder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=conf.CSV_PATH)
    if len(csv_folder) == 0:
        tm.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)

    return csv_folder


def create_rpt(parm_rst):
    if len(parm_rst) != 0:
        logger.info("--------------------------------------------")
        # 1. OVERALL REPORT
        # overall_rpt(parm_rst)
        mer_idst_stat(parm_rst)
        logger.info("--------------------------------------------")
        # 2. SUSPICIOUS DEVICE ANALYSIS
        # susp_device_stat(parm_rst)
        logger.info("--------------------------------------------")
        # 3. SUSPICIOUS MERCHANT ANALYSIS
        # susp_mer_stat(parm_rst)
    else:
        logger.info("parm_rst IS NULL, WRONG...")
    del parm_rst


def create_graph(parm_rst):
    parm_rst = pd.read_csv('C:/Users/wangrongkai/Desktop/111/test.csv', encoding='gbk')
    parm_rst["日期"] = pd.to_datetime(parm_rst["日期"])
    print(parm_rst.head())
    stem_cats = parm_rst.columns.tolist()
    fig = plt.figure(figsize=(20, 9))
    for sp in range(1, len(stem_cats)):
        ax = fig.add_subplot(3, 5, sp)
        ax.plot(parm_rst['日期'], parm_rst[stem_cats[sp]], linewidth=2)
        for key, spine in ax.spines.items():
            spine.set_visible(False)
        ax.tick_params(bottom="off", top="off", left="off", right="off")
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        ax.set_title(stem_cats[sp], fontsize=15)
        # plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%Y'))
    plt.gcf().autofmt_xdate()
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.subplots_adjust(top=0.9, bottom=0.1, left=0.05, right=0.98)
    plt.show()


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
    logger.info("PENDING CSV FILES LIST: %s" % csv_file_list)
    rst = data_preproc(csv_folder, csv_file_list)
    create_rpt(rst)
    logger.info("--------------------------------------------")
    logger.info('Main Processing Have Done...')


def init():
    logger.info("\n####LOG START####")
    logger.info("\n--------------------------------------------")
    logger.info("[1]. SYSTEM CONSTANT:")
    logger.info("LOG PATH: %s" % conf.LOG_PATH)
    logger.info("RESULT PATH: %s" % conf.RESULT_PATH)
    logger.info("[2]. BUSINESS CONSTANT:")
    logger.info("START DATE: %s" % conf.START_DATE)
    logger.info("END DATE: %s" % conf.END_DATE)
    logger.info("DATE RANGE: %s" % conf.DATE_RANGE)
    logger.info("ID HURDLE: %s" % conf.ID_HURDLE)
    logger.info("TRX HURDLE: %s" % conf.TRX_HURDLE)
    logger.info(idst_conf.mer_idst_dict)
    logger.info("--------------------------------------------")


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    conf = config.DeviceConfig()
    idst_conf = config.MerIndustryConfig
    logger = Logger(path=conf.LOG_PATH)
    init()
    create_graph(1)
    sys.exit(0)
    csv_folder = get_csv_folder()
    logger.info('CSV FILES FOLDER: %s' % csv_folder)
    main_process(csv_folder)
    end_time = datetime.datetime.now()
    logger.info("--------------------------------------------")
    logger.info("Total Processing Time:")
    logger.info('START TIME = %s' % start_time)
    logger.info('END TIME = %s' % end_time)
    logger.info('DIFF TIME = %s' % (end_time - start_time))
    logger.info('System Processing Have Done...')
