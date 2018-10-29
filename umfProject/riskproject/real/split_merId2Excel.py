#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/7/10 15:17
# @Author  : Wang Rongkai
# @Site    :
# @File    : split_merId2Excel.py
# @Software: PyCharm Community Edition

import datetime
import gc
import shutil
import logging
import os
import sys
import time
import tkinter.filedialog as tf
import tkinter.messagebox as tm
import pymysql
import traceback
import sqlalchemy as sq

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


def get_csv_folder():
    csv_folder = "D:/github_program/myPython/docs/csvfiles/todo_td/"
    # csv_folder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=conf.CSV_PATH)
    if len(csv_folder) == 0:
        tm.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)
    return csv_folder


def get_prod_id(row):
    a = row["支付产品编号"]
    if pd.isnull(a):
        pass
    elif len(a) > 8:
        return a[0:8]
    else:
        logger.info("PRODUCT ID ERROR...")
        pass


def main_process(parm_csv_floder):
    global TO_BE_BUSI_TYPE
    csv_file_list = os.listdir(parm_csv_floder)
    logger.info("PENDING CSV FILES LIST: %s" % csv_file_list)

    if TO_BE_BUSI_TYPE == conf.CST_CS:  # cs_call
        logger.info(conf.CST_CS)
        # cs_call_rst = cs_call.get_cs_call(csv_floder, csv_file_list)
        # insert_db(cs_call_rst, 'cs_call')
        # backup_csv(csv_floder, csv_file_list, "cs_call")
    elif TO_BE_BUSI_TYPE == conf.CST_CW:  # cs_warnmgt
        logger.info(conf.CST_CS)
        # cs_warnmgt_rst = cs_warn_mgt.get_cs_warn_mgt(csv_floder, csv_file_list)
        # insert_db(cs_warnmgt_rst, 'cs_warn_mgt')
        # backup_csv(csv_floder, csv_file_list, "cs_warn_mgt")
    elif TO_BE_BUSI_TYPE == conf.CST_DS:  # td_cust_trx_hist & ds_cap_rate & ds_recg_rate
        # trx_rst = td_cust_trx.get_cust_trx_hist(csv_floder, csv_file_list)
        tot_trx = split_mer_trx(parm_csv_floder, csv_file_list)
    logger.info('Main Processing Have Done...')


def get_trx_detail(parm_reader):
    all_trx = pd.DataFrame({})
    for chunk in parm_reader:
        x = chunk[["交易日期", "商户号", "产品号", "发卡行", "通道号",
                   "商户订单号", "内部流水号", "交易额（分）", "交易额（元）"]]
        all_trx = all_trx.append(x)
    return all_trx


def df_to_excel(parm_df, parm_path):
    logger.info("EXCEL PATH SAVE TO: %s" % parm_path)
    write = pd.ExcelWriter(parm_path)
    excel_header = ["交易日期", "商户号", "产品号", "发卡行", "通道号",
                    "商户订单号", "内部流水号", "交易额（分）", "交易额（元）"]
    parm_df.to_excel(write, sheet_name='Sheet1', header=excel_header, index=False)


def split_mer_trx(parm_csv_floder, parm_csv_file_list):
    logger.info("****PROCESSING CSV FILE FOR CAPTURE RATE START****")
    tot_mer_trx = pd.DataFrame({})
    tot_trx_cnt = 0
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join("%s%s%s" % (parm_csv_floder, "/", csv_file))
        # csv_file_path = "E:/myself/VBA/csv_files/pythonTest/test_01.csv"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding="gbk", chunksize=5000, iterator=True, dtype=str)

        # 3. 数据处理
        df_trx_detail = get_trx_detail(reader)
        tot_mer_trx = tot_mer_trx.append(df_trx_detail, ignore_index=True)

    # 4. 拆分商户号
    mer_id_list = tot_mer_trx.drop_duplicates(["商户号"])["商户号"].tolist()
    logger.info("==>> TOTAL MERCHANT TRANSACTION DETAILS COUNT: %s" % len(tot_mer_trx))
    logger.info("==>> TOTAL MERCHANT LIST: %s" % mer_id_list)
    for single_mer_id in mer_id_list:
        single_mer_df = tot_mer_trx[tot_mer_trx["商户号"] == single_mer_id]
        logger.info("==>> MERCHANT ID: %s" % single_mer_id)
        logger.info("==>> MERCHANT TRANSACTION COUNT: %s" % len(single_mer_df))
        tot_trx_cnt = tot_trx_cnt + len(single_mer_df)

        # 5. 保存至不同的EXCEL文件中
        excel_file_path = conf.RESULT_PATH + "rst_result_01_" + single_mer_id + ".xlsx"
        df_to_excel(single_mer_df, excel_file_path)
    logger.info("==>> TOTAL MERCHANT TRANSACTION DETAILS COUNT CHECK: %s" % tot_trx_cnt)
    logger.info("****PROCESSING CSV FILE FOR CAPTURE RATE END****")
    return tot_mer_trx


def init():
    global TO_BE_BUSI_TYPE, TO_BE_CSV_FLODER
    # TO_BE_BUSI_TYPE: CS, CW, DS
    TO_BE_BUSI_TYPE = "DS"
    TO_BE_CSV_FLODER = "E:/myself/VBA/csv_files/pythonTest"
    logger.info("\n####LOG START####")
    logger.info("\n--------------------------------------------")
    logger.info("[1]. SYSTEM CONSTANT:")
    logger.info("LOG PATH: %s" % conf.LOG_PATH)
    logger.info("RESULT PATH: %s" % conf.RESULT_PATH)
    logger.info("[2]. BUSINESS CONSTANT:")
    logger.info("TO_BE_BUSI_TYPE: %s" % TO_BE_BUSI_TYPE)
    logger.info("TO_BE_CSV_FLODER: %s" % TO_BE_CSV_FLODER)
    logger.info("--------------------------------------------")


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    conf = config.WeeklyStatConfig()
    logger = Logger(path=conf.LOG_PATH)
    init()
    # csv_folder = get_csv_folder()
    # logger.info('CSV FILES FOLDER: %s' % csv_folder)
    main_process(TO_BE_CSV_FLODER)
    end_time = datetime.datetime.now()
    logger.info("--------------------------------------------")
    logger.info("Total Processing Time:")
    logger.info('START TIME = %s' % start_time)
    logger.info('END TIME = %s' % end_time)
    logger.info('DIFF TIME = %s' % (end_time - start_time))
    logger.info('System Processing Have Done...')
