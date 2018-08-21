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


def get_point_csv(parm_df):
    pnt_df = pd.DataFrame({})
    logger.info("****MAKING POINT CSV FILE BEGIN****")
    label_list = ["mer_id", "mobile_no"]
    logger.info("[1]. POINT LABEL LIST:")
    logger.info(label_list)

    logger.info("[2]. GET POINT LABEL CSV:")
    for label in label_list:
        single_pnt_df = pd.DataFrame({})
        pnt_list = pd.Series(parm_df[label].unique()).tolist()
        logger.info("==>> LABEL: %s, COUNT: %s" % (label, len(pnt_list)))
        single_pnt_df["label"] = pd.Series(parm_df[label].unique())
        single_pnt_df["class"] = label
        pnt_df = pnt_df.append(single_pnt_df)
        del single_pnt_df

    logger.info("[3]. POINT[SHAPE]:")
    logger.info(pnt_df.shape)

    logger.info("[4]. SAVE POINT CSV FILE:")
    pnt_path = conf.RESULT_PATH + "pnt" + ".csv"
    pnt_df.to_csv(pnt_path, index=False)
    logger.info("SAVE TO: %s" % pnt_path)
    logger.info("****MAKING POINT CSV FILE END****")
    del pnt_df
    del parm_df


def get_relation_csv(parm_df):



    rlat_df = pd.DataFrame({})
    pnt_df = pd.DataFrame({})
    logger.info("****MAKING RELATION CSV FILE BEGIN****")
    label_list = ["mobile_no"]
    logger.info("[1]. RELATION LABEL LIST:")
    logger.info(label_list)

    # logger.info(parm_df[parm_df["mobile_no"] == "13013330277"])

    logger.info("[2]. GET RELATION LABEL CSV:")
    for label in label_list:
        single_pnt_df = pd.DataFrame({})
        logger.info("==>> LABEL: %s" % label)
        parm_dup = parm_df.drop_duplicates(["mer_id", label])
        single_rlat_df = pd.pivot_table(parm_dup, index=[label], values=["mer_id"],aggfunc=len).reset_index()

        logger.info(single_rlat_df.shape)
        logger.info(single_rlat_df)

        logger.info(len(single_rlat_df["mer_id"].unique().tolist()))

        pnd_lbl_list = pd.Series(single_rlat_df[single_rlat_df["mer_id"] > 1][label]).tolist()
        logger.info("==>> PENDING LABEL LENGTH: %s" % len(pnd_lbl_list))
        sys.exit(0)
        pnd_lbl_df = pd.DataFrame(parm_df[parm_df[label].isin(pnd_lbl_list)])
        pnd_rlat = pd.pivot_table(pnd_lbl_df, index=[label,"mer_id"], values=["inst_id"], aggfunc=len).reset_index()

        pnd_rlat.rename(columns={label: "source", "mer_id": "target", "inst_id": "weight"}, inplace=True)
        pnd_rlat["type"] = "Undirected"
        pnd_rlat["label"] = "mer_with_" + label
        rlat_df = rlat_df.append(pnd_rlat)

        pnt_list = pd.Series(pnd_lbl_df[label].unique()).tolist()
        logger.info("==>> LABEL: %s, COUNT: %s" % (label, len(pnt_list)))
        single_pnt_df["label"] = pd.Series(pnd_lbl_df[label].unique())
        single_pnt_df["class"] = label
        pnt_df = pnt_df.append(single_pnt_df)

        del single_rlat_df

    logger.info("[3]. SINGLE RELATION[SHAPE]:")
    print(pnt_df)
    logger.info(pnt_df.shape)

    logger.info("[3]. ALL RELATION[SHAPE]:")
    print(rlat_df)
    logger.info(rlat_df.shape)

    logger.info("[4]. SAVE RELATION CSV FILE:")
    rlat_path = conf.RESULT_PATH + "rlat" + ".csv"
    rlat_df.to_csv(rlat_path, index=False)
    logger.info("SAVE TO: %s" % rlat_path)
    logger.info("****MAKING RELATION CSV FILE END****")
    del rlat_df
    del parm_df


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
        csv_file_path = "D:/github_program/myPython/docs/csvfiles/201801/td_device_201801"
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
    csv_folder = "D:/github_program/myPython/docs/csvfiles/todo_td/"
    # csv_folder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=conf.CSV_PATH)
    if len(csv_folder) == 0:
        tm.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)

    return csv_folder


def create_rpt(parm_rst):
    if len(parm_rst) != 0:
        logger.info("--------------------------------------------")
        # 1. OVERALL REPORT
        # overall_rpt(parm_rst)
        logger.info("--------------------------------------------")
        parm_rst = parm_rst[parm_rst["mobile_no"] != "00000000000"]
        # 2. get_point_csv
        # get_point_csv(parm_rst)
        logger.info("--------------------------------------------")
        # 3. get_relation_csv
        # len(parm_rst["mer_id"].unique().tolist())
        logger.info(len(parm_rst["mer_id"].unique().tolist()))



        get_relation_csv(parm_rst)
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
    logger.info("ID HURDLE: %s" % conf.ID_HURDLE)
    logger.info("TRX HURDLE: %s" % conf.TRX_HURDLE)
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
