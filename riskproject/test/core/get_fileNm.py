#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/7 17:04
# @Author  : Wang Rongkai
# @Site    : 
# @File    : get_fileNm.py
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


def get_trx_detail(parm_reader):
    # global START_DATE
    rtn_df = pd.DataFrame({})
    print(parm_reader)

    for chunk in parm_reader:
        print(chunk)
        rtn_df = rtn_df.append(chunk)
    rtn_df.drop(["Unnamed: 10"], axis=1, inplace=True)
    # rtn_df.rename(columns={'商户号': 'mer_id'
    #     , '产品号': 'prod_id'
    #     , '交易日期': 'plat_date'
    #     , '成功交易笔数': 'success_cnt'
    #     , '拦截交易笔数': 'intercept_cnt'
    #     , '失败交易笔数': 'failed_cnt'
    #     , '成功交易金额': 'success_amt'
    #     , '拦截交易金额': 'intercept_amt'
    #     , '失败交易金额': 'failed_amt'}, inplace=True)
    rtn_df["batch_no"] = time.strftime("%Y%m%d%H%M%S")
    rst = pd.DataFrame(rtn_df.loc[:, ])
    return rst


def overall_rpt(parm_rst):
    global CAP_TRX_CNT, TOT_TRX_AMT
    logger.info("****OVERALL REPORT START****")
    logger.info("[1]. TOTAL TRANSACTION COUNT: %s" % len(parm_rst))

    logger.info("[2]. STATISTICS RANGE: FROM %s TO %s" %
                (str(parm_rst.sort_values(by="plat_date").head(1)["plat_date"].values)[2:12],
                 str(parm_rst.sort_values(by="plat_date").tail(1)["plat_date"].values)[2:12]))

    logger.info("[3]. TOTAL TRANSACTION AMOUNT: %s BILLION" % round(TOT_TRX_AMT / 1000000000, 2))

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
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/todo_td/NO 1_td_1"
        csv_file_path = "C:/Users/wangrongkai/Desktop/OTQ00005740+20180807162442/sql/NO 1_ CONDITION"
        # csv_file_path = "C:/Users/wangrongkai/Desktop/OTQ00005740+20180807162442/sql/1.txt"


        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding='utf-8', chunksize=50000, iterator=True, sep="|", dtype=str)



        # 3.  数据处理

        df_trx_detail = get_trx_detail(reader)
        logger.info("SINGLE CHUNK SHAPE:")
        logger.info(df_trx_detail.shape)
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
    csv_folder = "E:/SVN文档数据/08_产品风控/14.日常积累/10.自用SQL"
    # csv_folder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=conf.CSV_PATH)
    if len(csv_folder) == 0:
        tm.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)

    return csv_folder


def main_process(parm_csv_folder):
    csv_file_list = os.listdir(parm_csv_folder)
    # logger.info("PENDING CSV FILES LIST: %s" % csv_file_list)
    for csv_file in csv_file_list:
        logger.info(csv_file)
        # logger.info("")
        # logger.info("")
        # result = csv_file.find('.')
        # if result == -1:
        #     logger.info(csv_file)



    logger.info("--------------------------------------------")
    logger.info('Main Processing Have Done...')


def get_fileCnt(parm_csv_folder):
    csv_file_list = os.listdir(parm_csv_folder)



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
    # conn_neo4j()


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
