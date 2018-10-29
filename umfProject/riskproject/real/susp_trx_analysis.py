# -*- coding: utf-8 -*-
import datetime
import gc
import logging
import os
import sys
import time
import tkinter.filedialog as tf
import tkinter.messagebox as tm
import math
import traceback

import numpy as np
import pandas as pd

from riskproject.real import config
from itertools import combinations, permutations
from py2neo import Graph, Node, Relationship, Cursor


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
    rtn_df = pd.DataFrame({})
    for chunk in parm_reader:
        rtn_df = rtn_df.append(chunk)
    rtn_df["BATCH_NO"] = time.strftime("%Y%m%d%H%M%S")
    rtn_df["AMOUNT"] = rtn_df["AMOUNT"].astype(float)
    # rtn_df = rtn_df[(rtn_df["TRX_TIME"] >= conf.START_DATE) & (rtn_df["TRX_TIME"] <= conf.END_DATE)]
    return rtn_df


def overall_rpt(parm_rst):
    global CAP_TRX_CNT, TOT_TRX_AMT
    # date_range = 31
    logger.info("****OVERALL REPORT START****")
    logger.info(parm_rst.head(1)["plat_date"])
    logger.info(parm_rst.tail(1)["plat_date"])
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
    logger.info("TOP10 MERCHANT COUNT DETAILS SAVE TO %s" % mer_id_details_path)
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


def data_preproc(parm_csv_folder, parm_csv_file_list):
    global CAP_TRX_CNT, TOT_TRX_AMT, TO_BE_ANAL_LABEL
    CAP_TRX_CNT = 0.
    TOT_TRX_AMT = 0.
    rst = pd.DataFrame({})
    use_cols = ["INSTID", "TRACE", "ORDERID", "MERID", "PRODUCTID", "PLATDATE", "PLATTIME", "AMOUNT"] \
               + [TO_BE_ANAL_LABEL]
    # gc.set_debug(gc.DEBUG_STATS | gc.DEBUG_LEAK)
    gc.collect()
    gc.disable()
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_folder, '/', csv_file))
        logger.info("ORIGINAL FILE PATH: %s" % csv_file_path)
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/sim_0401-0415/td/NO 4_td"
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/sim_0401-0415/id/NO 3_id_4"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding='utf-8', chunksize=5000, iterator=True, sep="|", dtype=str,
                             parse_dates={"TRX_TIME": ["PLATDATE", "PLATTIME"]}, infer_datetime_format=True
                             , usecols=use_cols
                             )
        # 3.  数据处理
        df_trx_detail = get_trx_detail(reader)
        logger.info("SINGLE CHUNK SIZE: %s " % len(df_trx_detail))
        CAP_TRX_CNT = CAP_TRX_CNT + df_trx_detail.shape[0]
        TOT_TRX_AMT = TOT_TRX_AMT + df_trx_detail["AMOUNT"].sum()
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


def create_relation_csv(parm_rst):
    """
    Description:
    MAKING RELATION CSV FILES

    Parameters:
      :param parm_rst: 交易流水
      :param relt_label: 关联维度标签（手机号、身份证、IP地址、设备指纹）

    Process:

    1.统计1个设备在1个商户完成了几笔交易
      Return: td_mer_cnt
    2.找到关联设备，即：一个设备在2个或者2个以上的商户完成交易
      Return: relt_td_list
    3.根据关联设备，在(td_mer_cnt)中找到对应的商户
      Return: related_rst
    4.生成CSV文件供Gephi使用

    Returns:
      None

    Raises:
      IOError: An error occurred accessing the bigtable.Table object.
    """
    global TO_BE_ANAL_LABEL
    if len(parm_rst) != 0:
        # logger.info("\n")
        logger.info("****CREATE MERCHANT RELATION CSV FILE BEGIN****")
        logger.info("#### TO_BE_ANAL_LABEL: %s " % TO_BE_ANAL_LABEL)
        logger.info("#### ORIGINAL DATA SIZE: %s " % len(parm_rst))

        logger.info("[1]. FIND TRANSACTION COUNT OF EVERY %s IN DIFFERENT MERCHANT:" % TO_BE_ANAL_LABEL)
        td_mer_cnt = pd.pivot_table(parm_rst, index=[TO_BE_ANAL_LABEL, "MERID"], values=["TRACE"],
                                    aggfunc=len).reset_index()
        td_mer_cnt.rename(columns={TO_BE_ANAL_LABEL: "source", "MERID": "target", "TRACE": "weight"}, inplace=True)
        td_mer_cnt["type"] = "Undirected"
        td_mer_cnt["label"] = td_mer_cnt["source"]
        logger.info("==>> TRANSACTION COUNT: %s" % len(td_mer_cnt))

        logger.info("[2]. FIND RELATED %s:" % TO_BE_ANAL_LABEL)
        relt_td_df = pd.pivot_table(td_mer_cnt, index=["source"], values=["target"], aggfunc=len).reset_index()
        relt_td_list = pd.Series(relt_td_df[relt_td_df["target"] > 3]["source"]).tolist()
        del relt_td_df
        logger.info("==>> RELATED %s COUNT: %s" % (TO_BE_ANAL_LABEL,len(relt_td_list)))

        logger.info("[3]. FIND RELATED MERCHANT BY RELATED %s:" % TO_BE_ANAL_LABEL)
        related_rst = pd.DataFrame(td_mer_cnt[td_mer_cnt["source"].isin(relt_td_list)])
        logger.info("==>> RELATED MERCHANT BY RELATED %s DF SIZE: %s" % (TO_BE_ANAL_LABEL, len(related_rst)))

        logger.info("[4]. SAVE THE RELATION:")
        relation_save_path = conf.RESULT_PATH + "relation_of_" + TO_BE_ANAL_LABEL + ".csv"
        logger.info("SAVE TO: %s" % relation_save_path)
        related_rst.to_csv(relation_save_path, index=False)
        logger.info("****CREATE MERCHANT RELATION CSV FILE END****")
        logger.info("--------------------------------------------")
    else:
        logger.info("parm_rst IS NULL, WRONG...")
    del parm_rst


def create_rpt(parm_rst):
    if len(parm_rst) != 0:
        logger.info("--------------------------------------------")
        # 1. OVERALL REPORT
        # overall_rpt(parm_rst)
        logger.info("--------------------------------------------")
        # 2. MAKING RELATION CSV FILE
        relt_df = pd.DataFrame(parm_rst[["TRACE", "MERID", "AMOUNT", TO_BE_ANAL_LABEL]].loc[:, ])
        create_relation_csv(relt_df)
        del parm_rst
    else:
        logger.info("parm_rst IS NULL, WRONG...")

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
    global TO_BE_ANAL_LABEL
    # TO_BE_ANAL_LABEL: MOBILEID, IDENTIFYCODE, ORDERID, TD_DEVICEID, ZY_DEVICEID
    TO_BE_ANAL_LABEL = "IDENTIFYCODE"
    logger.info("\n####LOG START####")
    logger.info("\n--------------------------------------------")
    logger.info("[1]. SYSTEM CONSTANT:")
    logger.info("LOG PATH: %s" % conf.LOG_PATH)
    logger.info("RESULT PATH: %s" % conf.RESULT_PATH)
    logger.info("[2]. BUSINESS CONSTANT:")
    logger.info("START DATE: %s" % conf.START_DATE)
    logger.info("END DATE: %s" % conf.END_DATE)
    logger.info("DATE RANGE: %s" % conf.DATE_RANGE.days)
    logger.info("--------------------------------------------")


if __name__ == '__main__':
    # import mer_sim_analysis
    # print(help(mer_sim_analysis))
    start_time = datetime.datetime.now()
    conf = config.SimilarityConfig()
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
