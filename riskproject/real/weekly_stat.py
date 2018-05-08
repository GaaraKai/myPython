# coding:utf-8
# -*- coding: utf-8 -*-
# @Time    : 2018/5/8 10:10
# @Author  : Wang Rongkai
# @Site    : 
# @File    : weekly_stat.py
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


def insert_db(parm_df, parm_table):
    engine = sq.create_engine('mysql+pymysql://root:root@127.0.0.1:3306/rcdb?charset=utf8',
                              echo=True,
                              encoding='utf-8')
    try:
        parm_df.to_sql(name=parm_table, con=engine, if_exists='append', index=False, index_label=False)
        logger.info('DataBase Processing Success...')
    except:
        logger.info("DataBase Processing Error...")
        tm.showinfo(title='错误提示', message='数据库操作出现错误')
        traceback.print_exc()
        sys.exit(0)


def backup_csv(parm_csv_floder, parm_csv_file_list, parm_floder_name):
    father_path = os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep + ".")
    new_floder = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + \
                 '\\docs\\csvfiles_bak\\' + parm_floder_name + '\\'
    zip_path = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + "\\docs\\bak\\" + \
               parm_floder_name + '_' + time.strftime("%Y%m%d%H%M%S")
    bak_floder = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\csvfiles_bak\\'

    # 1、判断csvfiles_bak文件夹是否存在，不存在时新建
    isExists = os.path.exists(new_floder)
    if not isExists:
        os.makedirs(new_floder)

    # 2、将csvfile对应类型文件夹下的文件剪切到csvfiles_bak文件夹下
    for csv_file in parm_csv_file_list:
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_floder, '/', csv_file))
        new_file_path = new_floder + csv_file
        # logger.info("original csv files path = %s" % csv_file_path)
        # logger.info("move to ==>>", new_file_path)
        shutil.copy(csv_file_path, new_floder)
        os.remove(csv_file_path)

    # 3、将csvfiles_bak文件夹下对应文件夹打zip包放入/docs/bak下
    # logger.info("ready to zip csv floder ==>> ", new_floder)
    # logger.info("target zip file path ==>> ", zip_path)
    # logger.info("bak_floder ==>> ", bak_floder)
    shutil.make_archive(zip_path, 'zip', root_dir=new_floder)

    # 4、移除刚刚新建的csvfiles_bak文件夹（备份文件已经打包放入bak下，过渡文件可以删除）
    shutil.rmtree(bak_floder)
    logger.info('Backup CSV Files Processing Done...')


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
        cap_rst = get_ds_cap_rate(parm_csv_floder, csv_file_list)
        recg_rst = get_ds_recg_rate(parm_csv_floder, csv_file_list)
        # insert_db(cap_rst, 'ds_cap_rate')
        # insert_db(recg_rst, 'ds_recg_rate')
        # backup_csv(csv_floder, csv_file_list, "ds_stat")
    logger.info('Main Processing Have Done...')


def get_recg_rate(parm_reader):
    all_trx = pd.DataFrame({})
    prod_id = pd.Series()
    for chunk in parm_reader:
        x = chunk[["机构号", "同盾设备指纹", "自研设备指纹", "平台日期"]]
        all_trx = all_trx.append(x)
        prod_id_list = chunk.apply(get_prod_id, axis=1)
        prod_id = prod_id.append(prod_id_list)
        all_trx["prod_id"] = prod_id
    # 1. 数据准备，得到所有交易
    all_trx.rename(columns={"机构号": "inst_id",
                            "同盾设备指纹": "td_device",
                            "自研设备指纹": "zy_device",
                            "平台日期": "plat_date"}, inplace=True)

    # 2. 获取交易ID，时间范围
    rtn_prod_id = all_trx.iloc[0]["prod_id"]
    all_trx_sort = all_trx.sort_values(by=["plat_date"], ascending=True)
    start_date = all_trx_sort.iloc[0]["plat_date"].replace("-","")
    end_date = all_trx_sort.iloc[-1]["plat_date"].replace("-","")
    rtn_date_range = start_date + "-" + end_date

    # 3. 排除异常数据（空值，111111,000000）
    all_trx = all_trx.dropna(axis=0, how="any")
    all_trx = all_trx[all_trx["td_device"] != "111111"]
    all_trx = all_trx[all_trx["zy_device"] != "000000"]

    # 4. 统计自研识别后，认为是同一设备的设备个数（zy_recg_cnt）
    trx_cnt = pd.pivot_table(all_trx, index=["zy_device"], values="inst_id", aggfunc=len)
    if len(trx_cnt) ==0:
        rst = pd.DataFrame({"date_range": [rtn_date_range],
                            "prod_id": [rtn_prod_id],
                            "td_recg_cnt": 0,
                            "zy_recg_cnt": 0, }, index=None)
    else:
        trx_cnt = trx_cnt[trx_cnt["inst_id"] > 1]
        zy_recg_cnt = len(trx_cnt)

        # 5. 找到自研认为是同一设备的设备指纹（zy_dup_list）
        trx_cnt = trx_cnt.reset_index()
        zy_dup_list = trx_cnt[["zy_device"]]

        # 6.  找到自研认为是同一设备的设备指纹所对应的同盾设备指纹，并统计其个数（td_recg_cnt）
        td_di = pd.merge(all_trx,zy_dup_list,how="inner",on="zy_device")
        td_di = td_di.drop_duplicates(["td_device"])
        td_recg_cnt = len(td_di)

        # 7.  合并DF返回
        rst = pd.DataFrame({"date_range": [rtn_date_range],
                            "prod_id": [rtn_prod_id],
                            "td_recg_cnt": [td_recg_cnt],
                            "zy_recg_cnt": [zy_recg_cnt],}, index=None)
    return rst


def get_ds_recg_rate(parm_csv_floder, parm_csv_file_list):
    logger.info("****PROCESSING CSV FILE FOR RECOGNIZE RATE START****")
    rst_recg_rate = pd.DataFrame({})
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join("%s%s%s" % (parm_csv_floder, "/", csv_file))
        # csv_file_path = "E:/myself/VBA/csv_files/source\ds/0427-0503/DS_0427-0503_094955.csv"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding="gbk", chunksize=5000, iterator=True, dtype=str)

        # 3. 数据处理
        df_recg_rate = get_recg_rate(reader)
        rst_recg_rate = rst_recg_rate.append(df_recg_rate, ignore_index=True)
    rst_recg_rate["batch_no"] = time.strftime("%Y%m%d%H%M%S")
    logger.info("==>> RECOGNIZE RATE RESULT SIZE: %s" % len(rst_recg_rate))
    logger.info("****PROCESSING CSV FILE FOR RECOGNIZE RATE END****")
    return rst_recg_rate


def get_cap_rate(parm_reader):
    all_trx = pd.DataFrame({})
    prod_id = pd.Series()
    for chunk in parm_reader:
        x = chunk[["机构号", "同盾设备指纹", "自研设备指纹", "平台日期"]]
        all_trx = all_trx.append(x)
        prod_id_list = chunk.apply(get_prod_id, axis=1)
        prod_id = prod_id.append(prod_id_list)
        all_trx["prod_id"] = prod_id
    all_trx.rename(columns={"机构号": "inst_id",
                            "同盾设备指纹": "td_device",
                            "自研设备指纹": "zy_device",
                            "平台日期": "plat_date"}, inplace=True)
    trx_cnt = pd.pivot_table(all_trx, index=["plat_date", "prod_id"], values="inst_id", aggfunc=len)
    trx_cnt.rename(columns={"inst_id": "trx_cnt"}, inplace=True)
    # 1. 排除异常数据（空值，111111,000000）
    all_trx_dropna = all_trx.dropna(axis=0, how="any")
    all_trx_drop11 = all_trx_dropna[all_trx_dropna["td_device"] != "111111"]
    all_trx_drop00 = all_trx_dropna[all_trx_dropna["zy_device"] != "000000"]
    # 2. 统计获取数
    td_cap_cnt = pd.pivot_table(all_trx_drop11, values="td_device", index=["plat_date", "prod_id"], aggfunc=len)
    zy_cap_cnt = pd.pivot_table(all_trx_drop00, values="zy_device", index=["plat_date", "prod_id"], aggfunc=len)

    # 3. 重置索引
    trx_cnt = trx_cnt.reset_index()
    td_cap_cnt = td_cap_cnt.reset_index()
    zy_cap_cnt = zy_cap_cnt.reset_index()

    # 4. 合并DF
    if len(td_cap_cnt) == 0:
        merge_td = trx_cnt
        merge_td["td_device"] = 0
        logger.info("merge_td %s \n" % merge_td)
    else:
        merge_td = pd.merge(trx_cnt, td_cap_cnt, how="left", on=["plat_date", "prod_id"])

    if len(zy_cap_cnt) == 0:
        merge_zy = merge_td
        merge_zy["zy_device"] = 0
        logger.info("merge_zy %s \n" %  merge_zy)
    else:
        merge_zy = pd.merge(merge_td, zy_cap_cnt, how="left", on=["plat_date", "prod_id"])
    merge_zy.rename(columns={"td_device": "td_cap_cnt", "zy_device": "zy_cap_cnt"}, inplace=True)

    return merge_zy


def get_ds_cap_rate(parm_csv_floder, parm_csv_file_list):
    logger.info("****PROCESSING CSV FILE FOR CAPTURE RATE START****")
    rst_cap_rate = pd.DataFrame({})
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join("%s%s%s" % (parm_csv_floder, "/", csv_file))
        # csv_file_path = "E:/myself/VBA/csv_files/source\ds/0427-0503/DS_0427-0503_094955.csv"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding="gbk", chunksize=5000, iterator=True, dtype=str)

        # 3. 数据处理
        df_cap_rate = get_cap_rate(reader)
        rst_cap_rate = rst_cap_rate.append(df_cap_rate, ignore_index=True)

    # 4. 计算获取率
    rst_cap_rate["td_cap_rate"] = rst_cap_rate["td_cap_cnt"] / rst_cap_rate["trx_cnt"]
    rst_cap_rate["zy_cap_rate"] = rst_cap_rate["zy_cap_cnt"] / rst_cap_rate["trx_cnt"]
    rst_cap_rate["batch_no"] = time.strftime("%Y%m%d%H%M%S")
    logger.info("==>> CAPTURE RATE RESULT SIZE: %s" % len(rst_cap_rate))
    logger.info("****PROCESSING CSV FILE FOR CAPTURE RATE END****")
    return rst_cap_rate


def init():
    global TO_BE_BUSI_TYPE, TO_BE_CSV_FLODER
    # TO_BE_BUSI_TYPE: CS, CW, DS
    TO_BE_BUSI_TYPE = "DS"
    TO_BE_CSV_FLODER = "E://myself//VBA//csv_files//source//ds//0427-0503"

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
