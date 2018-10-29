# -*- coding: utf-8 -*-
import datetime
import gc
import logging
import os
import sys
import time
import tkinter.filedialog as tf
import tkinter.messagebox as tm

import numpy as np
import pandas as pd

from riskproject.real import config


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
    rtn_df = rtn_df[(rtn_df["plat_date"] >= conf.START_DATE) & (rtn_df["plat_date"] <= conf.END_DATE)]

    return rtn_df


def overall_rpt(parm_rst):
    global CAP_TRX_CNT, TOT_TRX_AMT
    logger.info("****OVERALL REPORT START****")
    logger.info("[1]. TOTAL DATA SHAPE:")
    logger.info(parm_rst.shape)
    logger.info("[2]. STATISTICS RANGE: FROM %s TO %s" % (conf.START_DATE, conf.END_DATE))
    total_trx_amount = round(TOT_TRX_AMT / 1000000000, 2)
    logger.info("[3]. TOTAL TRANSACTION COUNT: %s" % CAP_TRX_CNT)
    logger.info("[4]. TOTAL TRANSACTION AMOUNT: %s BILLION" % total_trx_amount)
    mer_id_cnt = np.array(parm_rst["mer_id"].unique())
    prod_id_cnt = pd.DataFrame(np.array(parm_rst["prod_id"].unique()).tolist()).dropna(axis=0, how='any').size
    device_id_cnt = pd.DataFrame(np.array(parm_rst["td_device"].unique()).tolist()).dropna(axis=0, how='any').size
    device_id_cnt_daily = round(device_id_cnt / conf.DATE_RANGE, 2)
    logger.info("[5]. TOTAL MERCHANT COUNT: %s" % mer_id_cnt.size)
    mer_id_details_path = conf.RESULT_PATH + "mer_id_details" + ".csv"
    # pd.DataFrame(rst["mer_id"].value_counts()).reset_index().head(10).to_csv(mer_id_details_path, index=False)
    logger.info("TOP10 MERCHANT COUNT DETAILS SAVE TO: %s" % mer_id_details_path)
    logger.info("[6]. TOTAL PRODUCT COUNT: %s" % prod_id_cnt)
    prod_id_details_path = conf.RESULT_PATH + "prod_id_details" + ".csv"
    # pd.DataFrame(rst["prod_id"].value_counts()).reset_index().to_csv(prod_id_details_path, index=False)
    logger.info("PRODUCT COUNT DETAILS SAVE TO: %s" % prod_id_details_path)
    logger.info("[7]. TOTAL DEVICE CAPTURE COUNT: %s" % device_id_cnt)
    logger.info("[8]. DAILY DEVICE CAPTURE COUNT: %s" % device_id_cnt_daily)
    td_device_details_path = conf.RESULT_PATH + "td_device_details" + ".csv"
    # pd.DataFrame(rst["td_device"].value_counts()).reset_index().head(10).to_csv(td_device_details_path, index=False)
    logger.info("TOP10 DEVICE COUNT DETAILS SAVE TO %s" % td_device_details_path)
    logger.info("****OVERALL REPORT END****")


def device_analysis(parm_df):
    logger.info("****TD_DEVICE ANALYSIS BEGIN****")
    logger.info("[1]. FIND TD_DEVICE TRANSACTION COUNT[TOP5]:")
    td_device_with_trx_cnt = pd.pivot_table(parm_df, index=["td_device"], values=["inst_id"], aggfunc=len) \
        .sort_values(by='inst_id', axis=0, ascending=False).reset_index()
    td_device_with_trx_cnt.rename(columns={"inst_id": "trx_cnt"}, inplace=True)
    logger.info(td_device_with_trx_cnt.head())

    logger.info("[2]. FIND TD_DEVICE TRANSACTION AMOUNT[TOP5]:")
    td_device_with_trx_amt = pd.pivot_table(parm_df, index=["td_device"], values=["trx_amount"], aggfunc=np.sum) \
        .sort_values(by='trx_amount', axis=0, ascending=False).reset_index()
    logger.info(td_device_with_trx_amt.head())

    logger.info("[3]. MERGE TRANSACTION COUNT & AMOUNT BY TD_DEVICE[TOP5]:")
    merge_rst = pd.merge(td_device_with_trx_cnt, td_device_with_trx_amt, how='inner', on=['td_device'])
    merge_rst["avg_trx_amt_by_one_device"] = \
        round(merge_rst["trx_amount"] / merge_rst["trx_cnt"], 0)
    logger.info(merge_rst.head())
    logger.info("SHAPE:")
    logger.info(merge_rst.shape)

    logger.info("[4]. SAVE TD_DEVICE TRANSACTION DETAILS:")
    merge_rst.sort_values(by='trx_amount', axis=0, ascending=False, inplace=True)
    merge_rst_path = conf.RESULT_PATH + "device_bl_analysis.csv"
    logger.info("SAVE TO: %s" % merge_rst_path)
    merge_rst.to_csv(merge_rst_path, index=False)

    pnd_td_device_file = conf.CONF_PATH + "pnd_td_device.csv"
    logger.info("[5]. PENDING TD_DEVICE FILE: %s" % pnd_td_device_file)
    pnd_td_device_list = pd.read_csv(pnd_td_device_file)["sup_td_device"].tolist()
    if len(pnd_td_device_list) == 0:
        logger.info("==>> NO PENDING TD_DEVICE")
    else:
        logger.info("==>> PENDING TD_DEVICE COUNT: %s" % len(pnd_td_device_list))
        logger.info("==>> PENDING TD_DEVICE DETAILS[TOP3]: %s" % pnd_td_device_list[0:3])
        logger.info("==>> RESULT OF PENDING TD_DEVICE TRANSACTION COUNT & AMOUNT[TOP5]:")
        td_device_trx_stat = pd.DataFrame(merge_rst[merge_rst["td_device"].isin(pnd_td_device_list)])
        logger.info(td_device_trx_stat.head())
    logger.info("****TD_DEVICE ANALYSIS END****")
    del parm_df


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
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/201801/td_device_201801"

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
        overall_rpt(parm_rst)
        logger.info("--------------------------------------------")
        # 2. DEVICE ANALYSIS
        device_analysis(parm_rst)
        logger.info("--------------------------------------------")
    else:
        logger.info("parm_rst IS NULL, WRONG...")
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
    logger.info("--------------------------------------------")


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    conf = config.DeviceConfig()
    logger = Logger(path=conf.LOG_PATH)
    init()
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
