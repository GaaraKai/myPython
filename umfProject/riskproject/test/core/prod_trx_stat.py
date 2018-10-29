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
    print(common_log() + '-->>' + 'original parm_df size ==>> \n ', len(parm_df))
    print(common_log() + '-->>' + 'target table name = ', parm_table)
    engine = sq.create_engine('mysql+pymysql://root:root@127.0.0.1:3306/rcdb?charset=utf8',
                              # echo=True,
                              encoding='utf-8')
    try:
        parm_df.to_sql(name=parm_table, con=engine, if_exists='append', index=False, index_label=False)
        print('DataBase Processing Success...')
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
    rtn_df.drop(["Unnamed: 9"], axis=1, inplace=True)
    rtn_df.rename(columns={'商户号': 'mer_id'
        , '产品号': 'prod_id'
        , '交易日期': 'plat_date'
        , '成功交易笔数': 'success_cnt'
        , '拦截交易笔数': 'intercept_cnt'
        , '失败交易笔数': 'failed_cnt'
        , '成功交易金额': 'success_amt'
        , '拦截交易金额': 'intercept_amt'
        , '失败交易金额': 'failed_amt'}, inplace=True)
    rtn_df["batch_no"] = time.strftime("%Y%m%d%H%M%S")
    rtn_df["plat_date"] = pd.to_datetime(rtn_df["plat_date"])
    rtn_df['success_cnt'] = rtn_df['success_cnt'].astype("uint32")
    rtn_df['intercept_cnt'] = rtn_df['intercept_cnt'].astype("uint32")
    rtn_df['failed_cnt'] = rtn_df['failed_cnt'].astype("uint32")

    rtn_df['success_amt'] = rtn_df['success_amt'].astype("float64")
    rtn_df['intercept_amt'] = rtn_df['intercept_amt'].astype("float64")
    rtn_df['failed_amt'] = rtn_df['failed_amt'].astype("float64")
    rtn_df = rtn_df.dropna(axis=0)
    # rtn_df = rtn_df[(rtn_df["plat_date"] >= conf.START_DATE) & (rtn_df["plat_date"] <= conf.END_DATE)]
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
        csv_file_path = "D:/github_program/myPython/docs/csvfiles/prod_trx_stat/0629-0802/NO 1_0629-0802"
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/todo_td/NO 1_td_1"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding='utf-8', chunksize=50000, iterator=True, sep="|", dtype=str)

        # 3.  数据处理
        df_trx_detail = get_trx_detail(reader)
        logger.info("SINGLE CHUNK SHAPE:")
        logger.info(df_trx_detail.shape)
        CAP_TRX_CNT = CAP_TRX_CNT + len(df_trx_detail)
        TOT_TRX_AMT = TOT_TRX_AMT + df_trx_detail["success_amt"].sum()
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
    csv_folder = "D:/github_program/myPython/docs/csvfiles/todo_td/"
    # csv_folder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=conf.CSV_PATH)
    if len(csv_folder) == 0:
        tm.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)

    return csv_folder


def get_top(parm_rst, parm_dimen, param_col):
    parm_rst.sort_values(by=param_col, ascending=False, inplace=True)
    return parm_rst[parm_dimen].head(10).tolist()


def stat_of_top(parm_dimen, parm_rst):
    logger.info("****%s STATISTIC START****" % parm_dimen.upper())
    tot_cnt = pd.DataFrame(np.array(parm_rst[parm_dimen].unique()).tolist()).size
    logger.info("[1]. TOTAL %s COUNT: %s " % (parm_dimen.upper(), tot_cnt))
    logger.info("[2]. TOP10 %s:" % parm_dimen.upper())
    dimen_trx_cnt = pd.pivot_table(parm_rst, index=[parm_dimen],aggfunc=np.sum).reset_index()
    logger.info("==>> FAILED: %s " % get_top(dimen_trx_cnt, parm_dimen, "failed_cnt"))
    logger.info("==>> INTERCEPT: %s " % get_top(dimen_trx_cnt, parm_dimen, "intercept_cnt"))
    logger.info("==>> SUCCESS: %s " % get_top(dimen_trx_cnt, parm_dimen, "success_cnt"))
    tot_list = list_concat(get_top(dimen_trx_cnt, parm_dimen, "failed_cnt"),
                           get_top(dimen_trx_cnt, parm_dimen, "intercept_cnt"),
                           get_top(dimen_trx_cnt, parm_dimen, "success_cnt"))
    logger.info("[3]. TOTAL PENDING %s LIST COUNT: %s " % (parm_dimen.upper(), len(tot_list)))
    logger.info("==>> TOTAL PENDING %s DETAILS: %s " % (parm_dimen.upper(), tot_list))
    rst = pd.DataFrame(parm_rst[parm_rst[parm_dimen].isin(tot_list)])
    logger.info("==>> TOTAL PENDING %s TRANSACTIONS CNT: %s" % (parm_dimen.upper(), len(rst)))
    csv_save_path = conf.RESULT_PATH + parm_dimen + "_trx_details" + ".csv"
    rst.to_csv(csv_save_path, index=False)
    logger.info("[4]. TOTAL PENDING %s TRANSACTIONS SAVE TO: %s" % (parm_dimen.upper(), csv_save_path))
    logger.info("****%s STATISTIC END****" % parm_dimen.upper())

    return rst


def create_rpt(parm_rst):
    if len(parm_rst) != 0:
        logger.info("--------------------------------------------")
        # 1. OVERALL REPORT
        overall_rpt(parm_rst)
        logger.info("--------------------------------------------")
        # 2. STATISTICS OF MERCHANT
        stat_of_top("mer_id", parm_rst)
        logger.info("--------------------------------------------")
        # 3. STATISTICS OF PRODUCT
        stat_of_top("prod_id", parm_rst)
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
    # print(rst.dtypes)
    # sys.exit(0)
    logger.info("rst\n %s" % rst)
    insert_db(rst, "stat_mer")
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
    logger.info("--------------------------------------------")
    # conn_neo4j()


if __name__ == '__main__':
    # global g
    # import mer_sim_analysis
    # print(help(mer_sim_analysis))
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
