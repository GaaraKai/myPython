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
import logging
import configparser


class Logger:
    ASD = "11"
    path = "C:\\Users\\wangrongkai\\Desktop\\123\\example.log"
    def __init__(self, path = path, slevel=logging.DEBUG, flevel=logging.DEBUG):
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

    def ex(self):
        self.oo = 'this is test'

def get_trx_detail(parm_reader):
    global START_DATE
    rtn_df = pd.DataFrame({})
    for chunk in parm_reader:
        rtn_df = rtn_df.append(chunk)
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
    # logger.info(rtn_df.tail())
    # logger.info("ALL rtn_df.shape = ", rtn_df.shape)
    rtn_df["plat_date"] = pd.to_datetime(rtn_df["plat_date"])
    rtn_df = rtn_df[rtn_df["plat_date"] >= START_DATE]

    return rtn_df


def susp_device_stat(parm_df):
    global RST_PATH, ID_HURDLE
    logger.info("****SUSPICIOUS DEVICE ANALYSIS BEGIN****")
    logger.info("[1]. ALL TRANSACTION WITH DROP NULL ID_NO[SHAPE]:")
    rst_dropna_id_no = parm_df.dropna(axis=0, subset=["id_no"])
    logger.info(rst_dropna_id_no.shape)
    logger.info("[2]. AFTER [1], DROP DUPLICATED TD_DEVICE & ID_NO[SHAPE]:")
    rst_dropna_id_no_dup = rst_dropna_id_no.drop_duplicates(["td_device", "id_no"])
    logger.info(rst_dropna_id_no_dup.shape)  # (90288, 16)
    logger.info("[3]. FIND COUNT OF ID_NO BY EVERY DEVICE[TOP5]:")
    devi_with_id_cnt = pd.pivot_table(rst_dropna_id_no_dup, index=["td_device"], values=["id_no"], aggfunc=len) \
        .sort_values(by='id_no', axis=0, ascending=False).reset_index()
    devi_with_id_cnt.rename(columns={"id_no": "id_no_cnt"}, inplace=True)
    logger.info(devi_with_id_cnt.head())
    logger.info("[4]. FIND SUSPICIOUS DEVICE WHERE ID HURDLE IS[%s]:" % ID_HURDLE)
    susp_device_list = pd.Series(devi_with_id_cnt[devi_with_id_cnt["id_no_cnt"] >= ID_HURDLE]["td_device"]) \
        .tolist()
    if len(susp_device_list) == 0:
        logger.info("NO SUSPICIOUS DEVICE BECAUSE ID HURDLE %s IS TOO HIGH." % ID_HURDLE)
    else:
        logger.info("==>> SUSPICIOUS DEVICE: %s" % susp_device_list)
        logger.info("[5]. FIND MERCHANT WHIT SUSPICIOUS DEVICE:")
        mer_with_susp_device = pd.DataFrame(
            rst_dropna_id_no_dup[rst_dropna_id_no_dup["td_device"].isin(susp_device_list)])
        mer_with_susp_device_list = mer_with_susp_device["mer_id"].unique().tolist()
        logger.info("==>> SUSPICIOUS MERCHANT: %s" % (mer_with_susp_device_list))
        mer_with_susp_device_detl = pd.pivot_table(mer_with_susp_device,
                                                   values=["inst_id"],
                                                   index=["mer_id", "td_device", "id_no"],
                                                   aggfunc=len).sort_values(by='inst_id', axis=0,
                                                                            ascending=False).reset_index()
        logger.info("[6]. FIND SUSPICIOUS DEVICE TRANSACTION DETAILS:")
        logger.info("==>> SUSPICIOUS TRANSACTION[e.g.]:")
        logger.info(mer_with_susp_device_detl.head())
        logger.info("[7]. SAVE SUSPICIOUS DEVICE TRANSACTION DETAILS:")
        mer_with_susp_device_trx_details_path = RST_PATH + "mer_with_susp_device_trx_details" + ".csv"
        # mer_with_susp_device.to_csv(mer_with_susp_device_trx_details_path, index=False)
        logger.info("SAVE TO: %s" % mer_with_susp_device_trx_details_path)
    logger.info("****SUSPICIOUS DEVICE ANALYSIS END****")

    del parm_df


def susp_mer_stat(parm_rst):
    global RST_PATH, TRX_HURDLE
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
    mer_with_susp_device_trx_details_path = RST_PATH + "mer_with_susp_device_trx_details" + ".csv"
    # mer_with_susp_device.to_csv(mer_with_susp_device_trx_details_path, index=False)
    logger.info("SAVE TO: %s" % (mer_with_susp_device_trx_details_path))
    logger.info("[5] FIND MERCHANT WHERE TRANSACTION COUNT OVER HURDLE[%s]:" % TRX_HURDLE)
    key_mer_by_trx_list = pd.Series(
        mer_with_trx_cnt[mer_with_trx_cnt["trx_cnt"] >= TRX_HURDLE]["mer_id"]).tolist()
    if len(key_mer_by_trx_list) == 0:
        logger.info("==>> NO MERCHANT BECAUSE TRX_HURDLE [%s] IS TOO HIGH." % TRX_HURDLE)
    else:
        logger.info("==>> MONITOR MERCHANT COUNT: %s" % (len(key_mer_by_trx_list)))
        logger.info("==>> MONITOR MERCHANT DETAILS: %s" % (key_mer_by_trx_list))
        logger.info("==>> MONITOR MERCHANT TRANSACTION COUNT[OVER HURDLE]:")
        key_mer_trx_stat = pd.DataFrame(merge_rst[merge_rst["mer_id"].isin(key_mer_by_trx_list)])
        logger.info(key_mer_trx_stat)
    avg_trx_top = 5
    logger.info("[6] FIND MERCHANT WHERE AVERAGE TRANSACTION COUNT WITH DEVICE[TOP %s]:" % avg_trx_top)
    key_mer_avg_stat = pd.DataFrame(
        merge_rst.sort_values(by='avg_trx_cnt_by_one_device', axis=0, ascending=False))[0:avg_trx_top]
    key_mer_by_avg_list = pd.Series(key_mer_avg_stat["mer_id"]).tolist()
    logger.info("==>> SUSPICIOUS MERCHANT COUNT: %s" % (len(key_mer_by_avg_list)))
    logger.info("==>> SUSPICIOUS MERCHANT ID: %s" % (key_mer_by_avg_list))
    logger.info("==>> SUSPICIOUS MERCHANT TRANSACTION COUNT[TOP5]:")
    logger.info(key_mer_avg_stat)
    logger.info("****SUSPICIOUS MERCHANT ANALYSIS END****")

    del parm_rst


def overall_rpt(parm_rst):
    global CAP_TRX_CNT, TOT_TRX_AMT, START_DATE
    date_range = 31
    logger.info("****OVERALL REPORT START****")
    logger.info("[1]. TOTAL DATA SHAPE:")
    logger.info(parm_rst.shape)
    logger.info("[2]. STATISTICS RANGE: FROM %s TO NOW" % START_DATE)
    total_trx_amount = round(TOT_TRX_AMT / 1000000000, 2)
    logger.info("[3]. TOTAL TRANSACTION COUNT: %s" % CAP_TRX_CNT)
    logger.info("[4]. TOTAL TRANSACTION AMOUNT: %s BILLION" % total_trx_amount)
    mer_id_cnt = np.array(parm_rst["mer_id"].unique())
    prod_id_cnt = pd.DataFrame(np.array(parm_rst["prod_id"].unique()).tolist()).dropna(axis=0, how='any').size
    device_id_cnt = pd.DataFrame(np.array(parm_rst["td_device"].unique()).tolist()).dropna(axis=0, how='any').size
    device_id_cnt_daily = round(device_id_cnt / date_range, 2)
    logger.info("[5]. TOTAL MERCHANT COUNT: %s" % mer_id_cnt.size)
    mer_id_details_path = RST_PATH + "mer_id_details" + ".csv"
    # pd.DataFrame(rst["mer_id"].value_counts()).reset_index().head(10).to_csv(mer_id_details_path, index=False)
    logger.info("TOP10 MERCHANT COUNT DETAILS SAVE TO %s" % mer_id_details_path)
    logger.info("[6]. TOTAL PRODUCT COUNT: %s" % prod_id_cnt)
    prod_id_details_path = RST_PATH + "prod_id_details" + ".csv"
    # pd.DataFrame(rst["prod_id"].value_counts()).reset_index().to_csv(prod_id_details_path, index=False)
    logger.info("PRODUCT COUNT DETAILS SAVE TO: %s" % prod_id_details_path)
    logger.info("[7]. TOTAL DEVICE CAPTURE COUNT: %s" % device_id_cnt)
    logger.info("[8]. DAILY DEVICE CAPTURE COUNT: %s" % device_id_cnt_daily)
    td_device_details_path = RST_PATH + "td_device_details" + ".csv"
    # pd.DataFrame(rst["td_device"].value_counts()).reset_index().head(10).to_csv(td_device_details_path, index=False)
    logger.info("TOP10 DEVICE COUNT DETAILS SAVE TO %s" % td_device_details_path)
    logger.info("****OVERALL REPORT END****")


def data_preproc(parm_csv_folder, parm_csv_file_list):
    global CAP_TRX_CNT, TOT_TRX_AMT
    rst = pd.DataFrame({})

    # gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)
    gc.collect()
    gc.disable()
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_folder, '/', csv_file))
        logger.info("csv_file_path = %s" % (csv_file_path))
        csv_file_path = "D:/github_program/myPython/docs/csvfiles/201801/td_device_201801"
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/todo/NO 1_td_1"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding='utf-8', chunksize=5000, iterator=True, sep="|",dtype=str)

        # 3.  数据处理
        df_trx_detail = get_trx_detail(reader)
        logger.info("df_trx_detail = ")
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
    global csv_biz_type
    father_path = os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep + ".")
    default_dir = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\csvfiles\\'
    csv_folder = "D:/github_program/myPython/docs/csvfiles/todo/"
    # csv_folder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=(os.path.expanduser(default_dir)))
    if len(csv_folder) != 0:
        csv_biz_type = csv_folder.split('/')[-1]
        logger.info("csv_biz_type = %s" % (csv_biz_type))
    else:
        tm.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)

    return csv_folder


def create_rpt(parm_rst):
    if len(parm_rst) != 0:
        logger.info("--------------------------------------------")
        # 1. OVERALL REPORT
        overall_rpt(parm_rst)
        logger.info("--------------------------------------------")
        # 2. SUSPICIOUS DEVICE ANALYSIS
        susp_device_stat(parm_rst)
        logger.info("--------------------------------------------")
        # 3. SUSPICIOUS MERCHANT ANALYSIS
        susp_mer_stat(parm_rst)
    else:
        logger.info("WRONG")
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
    logger.info("csv_file_list = %s" % (csv_file_list))
    rst = data_preproc(csv_folder, csv_file_list)
    create_rpt(rst)
    logger.info("--------------------------------------------")
    logger.info('Main Processing Have Done...')


def init():
    # logger = Logger()
    global RST_PATH, START_DATE, TOT_TRX_AMT, CAP_TRX_CNT, ID_HURDLE, TRX_HURDLE, PRT_PATH
    father_path = os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep + ".")
    RST_PATH = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\rst\\'
    START_DATE = "2018-01-01"
    TOT_TRX_AMT = 0.
    CAP_TRX_CNT = 0.
    ID_HURDLE = 30
    TRX_HURDLE = 60000
    PRT_PATH = open("C:\\Users\\wangrongkai\\Desktop\\123\\example.log", "w")
    logger.info("--------------------------------------------")
    logger.info("CONSTANT:")
    logger.info("START_DATE: %s" % (START_DATE))
    logger.info("ID_HURDLE: %s" % (ID_HURDLE))
    logger.info("TRX_HURDLE: %s" % (TRX_HURDLE))
    logger.info("--------------------------------------------")


if __name__ == '__main__':
    logger = Logger()

    conf = configparser.ConfigParser()
    conf.read('config.ini')  # 文件路径
    a = conf.get("section1", "a")  # 获取指定section 的option值
    print(a)
    d = conf.get("section2", "d")  # 获取section1 的sex值
    print(d)

    sys.exit(0)
    init()
    start_time = datetime.datetime.now()
    csv_folder = get_csv_folder()
    logger.info('csv_folder = %s' % (csv_folder))
    main_process(csv_folder)
    end_time = datetime.datetime.now()
    logger.info('start_time = %s' % (start_time))
    logger.info('end_time   = %s' % (end_time))
    logger.info('diff_Time = %s' % (end_time - start_time))
    logger.info('System Processing Done...')

