#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/6/5 16:14
# @Author  : Wang Rongkai
# @Site    : 
# @File    : insert_spec_mer_trx_dtl.py
# @Software: PyCharm Community Edition


# -*- coding: utf-8 -*-
import datetime
import gc
import logging
import os
import sys
import time
import tkinter.filedialog as tf
import tkinter.messagebox as tm
import pymysql
import traceback
import sqlalchemy as sq

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


def common_log():
    return os.path.basename(__file__)


def list_concat(list_1, list_2, list_3):
    return list(set(list_1 + list_2 + list_3))


def insert_db(parm_df, parm_table):
    # print(parm_df.dtypes)
    # sys.exit(0)
    logger.info('original parm_df size ==>> %s ' % len(parm_df))
    logger.info('target table name = %s ' % parm_table)
    engine = sq.create_engine('mysql+pymysql://root:root@127.0.0.1:3306/rcdb?charset=utf8',
                              # echo=True,
                              encoding='utf-8')
    try:
        parm_df.to_sql(name=parm_table, con=engine, if_exists='append', index=False, index_label=False)
        logger.info('DataBase Processing Success...')
    except:
        print("DataBase Processing Error...")
        tm.showinfo(title='错误提示', message='数据库操作出现错误')
        traceback.print_exc()
        sys.exit(0)


def get_trx_detail(parm_reader):
    # global START_DATE
    rtn_df = pd.DataFrame({})
    for chunk in parm_reader:
        rtn_df = rtn_df.append(chunk)
    rtn_df = pd.DataFrame(rtn_df.loc[1:, ])

    # rtn_df.drop(["Unnamed: 9"], axis=1, inplace=True)
    rtn_df.rename(columns={'MERCHECKDATE': 'set_date'
        , 'INSTID': 'inst_id'
        , 'MERID': 'mer_id'
        , 'PRODUCTID': 'prod_id'
        , 'ISSUINGBANK': 'binbank_id'
        , 'BANKID': 'bank_id'
        , 'ORDERID': 'order_id'
        , 'TRACE': 'trace'
        , 'ORIGAMT': 'amount'
        , 'PLATDATE': 'plat_date'
        , 'PLATTIME': 'plat_time'}, inplace=True)
    rtn_df = rtn_df.dropna(axis=0)
    rtn_df["batch_no"] = time.strftime("%Y%m%d%H%M%S")
    rtn_df["set_date"] = pd.to_datetime(rtn_df["set_date"])
    rtn_df["plat_date"] = pd.to_datetime(rtn_df["plat_date"])
    rtn_df['amount'] = rtn_df['amount'].astype(float)
    rtn_df["amount"] = rtn_df["amount"]/100
    # rtn_df = rtn_df[(rtn_df["plat_date"] >= conf.START_DATE) & (rtn_df["plat_date"] <= conf.END_DATE)]
    rst = pd.DataFrame(rtn_df.loc[:, ])
    return rst

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
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/todo_td/NO 1_td_1"
        # csv_file_path = "D://github_program//myPython///docs//csvfiles//20180608//T_PTRANS_1303.out"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding='gbk', chunksize=50000, iterator=True,
                             dtype=str,
                             delim_whitespace=True)
        # 3.  数据处理
        df_trx_detail = get_trx_detail(reader)
        logger.info("SINGLE CHUNK SHAPE: %s" % len(df_trx_detail))
        CAP_TRX_CNT = CAP_TRX_CNT + len(df_trx_detail)
        # TOT_TRX_AMT = TOT_TRX_AMT + df_trx_detail["success_amt"].sum()
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
    csv_folder = "D://github_program//myPython///docs//csvfiles//20180608"

    # csv_folder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=conf.CSV_PATH)
    if len(csv_folder) == 0:
        tm.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)

    return csv_folder


def get_top(parm_rst, parm_dimen, param_col):
    parm_rst.sort_values(by=param_col, ascending=False, inplace=True)
    return parm_rst[parm_dimen].head(10).tolist()

def overall_rpt(parm_rst):
    global CAP_TRX_CNT, TOT_TRX_AMT
    logger.info("****OVERALL REPORT START****")
    logger.info("[1]. TOTAL TRANSACTION COUNT: %s" % len(parm_rst))
    parm_rst = parm_rst.drop_duplicates(['trace'])
    logger.info("[2]. TOTAL TRANSACTION COUNT & DROP DUP DATA: %s" % len(parm_rst))

    # logger.info("[2]. STATISTICS RANGE: FROM %s TO %s" %
    #             (str(parm_rst.sort_values(by="plat_date").head(1)["plat_date"].values)[2:12],
    #              str(parm_rst.sort_values(by="plat_date").tail(1)["plat_date"].values)[2:12]))
    #
    # logger.info("[3]. TOTAL TRANSACTION AMOUNT: %s BILLION" % round(TOT_TRX_AMT / 1000000000, 2))
    logger.info("[3]. SAVE THE RELATION:")
    pboc_save_path = conf.RESULT_PATH + "PBOC_" + "20180608" + ".csv"
    logger.info("SAVE TO: %s" % pboc_save_path)
    parm_rst.to_csv(pboc_save_path, index=False)
    logger.info("****OVERALL REPORT END****")


def create_rpt(parm_rst):
    if len(parm_rst) != 0:
        logger.info("--------------------------------------------")
        # 1. OVERALL REPORT
        overall_rpt(parm_rst)
        logger.info("--------------------------------------------")
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
    # logger.info("rst\n %s" % rst)
    create_rpt(rst)
    # insert_db(rst, "spec_mer_trx_dtl_0608")
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
    logger.info("--------------------------------------------")


if __name__ == '__main__':
    # global g
    # import mer_sim_analysis
    # print(help(mer_sim_analysis))
    start_time = datetime.datetime.now()
    conf = config.DeviceConfig()
    logger = Logger(path=conf.LOG_PATH)
    # init()
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
