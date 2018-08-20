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


def conn_neo4j():
    logger.info("Connect DB...")
    global g
    try:
        g = Graph(
            "http://localhost:7474",
            username="neo4j",
            password="china100!"
        )
        g.delete_all()
        logger.info("Delete all nodes & relations...")
        logger.info("Connect successfully...")
    except:
        print("Connect DB Error...")
        # traceback.print_exc()
        sys.exit(0)


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
    # rtn_df = rtn_df[(rtn_df["plat_date"] >= conf.START_DATE) & (rtn_df["plat_date"] <= conf.END_DATE)]

    return rtn_df


def get_trx_detail_new(parm_reader):
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
    # rtn_df = rtn_df[(rtn_df["plat_date"] >= conf.START_DATE) & (rtn_df["plat_date"] <= conf.END_DATE)]

    return rtn_df


def create_node(parm_node_1, parm_node_2, parm_relt, parm_line):
    tx = g.begin()
    # parm_line = [mer_id, dimension, weight]
    node1 = Node(parm_node_1, name=parm_line[0])
    node2 = Node(parm_node_2, name=parm_line[1])
    # rel = Relationship(node1, parm_relt, node2)
    rel = Relationship(node1, parm_relt, node2)
    rel["weight"] = str(parm_line[2])
    tx.merge(node1)
    tx.merge(node2)
    tx.merge(rel)
    tx.commit()


def get_susp_mer_dimen(parm_df):
    list_1 = pd.Series(parm_df["mer_a"]).tolist()
    list_2 = pd.Series(parm_df["mer_b"]).tolist()
    return list(set(list_1 + list_2))


def mer_sim_anls_preproc(parm_df, parm_label):
    """
        Description:
        MERCHANT SIMILARITY ANALYSIS DATA PRE-PROCESSING

        Parameters:
        :param parm_df: 交易流水df
        :param parm_label: 维度标签（手机号、身份证号、IP地址、设备指纹）

        Process:
        1.去掉空手机号的行记录
          Return: df_dropna
        2.根据“商户号+手机号”的维度去掉重复记录
          Return: df_dropdup
        3.找到一个商户号下，成功交易的手机号客户个数，此时的手机号已去重
          Return: mer_phone_cnt
        4.找到可疑手机号，如果1个手机号在2个或者2个以上的商户进行过交易，那么此手机号认为是可疑的手机号；
          Return: 可疑手机号列表(susp_phone_list)
        5.找到通过可疑手机号的交易流水
          Return: 可疑手机号流水(susp_phone_trx)
          交易流水中商户定义为待确认商户，即：用可疑手机号完成交易的商户
          Return: 待确认商户列表(pnd_mer_list)
        6.将待确认商户两两组合
          Return: 两两组合的商户列表(susp_mer_group_list)
        7.计算相似度指数(Tanimoto Coefficient)，得到待确认商户之间的相似度
          7.1 找到商户间手机号交易的交集
            a.将商户组里面的第1个商户去(susp_phone_trx)中找到此商户关联的手机号
              Return: a商户关联手机号列表(mer_a_phone_list)
            b.将商户组里面的第2个商户去susp_phone_trx中找到此商户关联的手机号
              Return: b商户关联手机号列表(mer_b_phone_list)
            c.计算：a商户和b商户手机号关联列表的交集手机号个数
              Return: inter_phone_cnt
          7.2 找到商户间手机号交易的并集
            a.将商户组里面的第1个商户去(susp_phone_trx)中找到此商户关联的手机号个数
              Return: a商户关联手机号列表(mer_a_tot_phone_cnt)
            b.将商户组里面的第2个商户去(susp_phone_trx)中找到此商户关联的手机号个数
              Return: b商户关联手机号列表(mer_b_tot_phone_cnt)
            c.计算：a商户和b商户手机号并集个数
              union_phone_cnt = mer_a_tot_phone_cnt + mer_b_tot_phone_cnt - inter_phone_cnt
              Return: union_phone_cnt
          7.3 计算相似度指数：TC
              Tanimoto Coefficient = inter_phone_cnt / union_phone_cnt
        8.汇总两两商户间的相似度指数
          Return: tc_rst

        Return: 
        df_dropna, tc_rst

        Raises:
            IOError: An error occurred accessing the bigtable.Table object.
    """
    tc_rst = pd.DataFrame({})
    # df_dropdup = pd.DataFrame({})
    logger.info("\n")
    logger.info("****MERCHANT SIMILARITY ANALYSIS DATA PRE-PROCESSING BEGIN****")

    logger.info("[1]. PARAMETERS:")
    logger.info("==>> ORIGINAL DATA FRAME SHAPE:")
    logger.info(parm_df.shape)
    logger.info("==>> SIMILARITY BASE TO: %s" % parm_label)

    logger.info("[2]. DROP NULL LABEL DATA:")
    df_dropna = parm_df.dropna(axis=0, subset=[parm_label])
    logger.info("==>> RESULT DATA FRAME SHAPE:")
    logger.info(df_dropna.shape)

    logger.info("[3]. DROP DUPLICATED DATA: (MERCHANT_ID & %s)" % parm_label)
    df_dropdup = df_dropna.drop_duplicates([parm_label, "mer_id"])
    logger.info("==>> RESULT DATA FRAME SHAPE:")
    logger.info(df_dropdup.shape)

    logger.info("[4]. FOR EACH MERCHANT, FIND DIFFERENT %s COUNT[e.g]:" % parm_label)
    mer_phone_cnt = pd.pivot_table(df_dropdup, index=["mer_id"], values=[parm_label], aggfunc=len).reset_index()
    logger.info(mer_phone_cnt.head(3))

    logger.info("[5]. FIND SUSPICIOUS %s WHICH RELATED DIFFERENT MERCHANT:" % parm_label)
    phone_with_diff_mer_cnt = \
        pd.pivot_table(df_dropdup, index=[parm_label], values=["mer_id"], aggfunc=len).reset_index()
    susp_phone_list = pd.Series(phone_with_diff_mer_cnt[phone_with_diff_mer_cnt["mer_id"] > 1][parm_label]).tolist()
    logger.info("==>> SUSPICIOUS %s COUNT: %s" % (parm_label, len(susp_phone_list)))

    logger.info("[6]. FIND SUSPICIOUS MERCHANT WITH SUSPICIOUS %s LIST:" % parm_label)
    # susp_phone_trx ==>> SUSPICIOUS PHONE TRANSACTIONS
    susp_phone_trx = pd.DataFrame(df_dropdup[df_dropdup[parm_label].isin(susp_phone_list)])

    pnd_mer_list = pd.Series(susp_phone_trx["mer_id"].unique()).tolist()
    logger.info("==>> SUSPICIOUS MERCHANT COUNT: %s" % len(pnd_mer_list))

    logger.info("[7]. GROUP SUSPICIOUS MERCHANT:")
    susp_mer_group_list = list(combinations(pnd_mer_list, 2))
    logger.info("==>> TOTAL GROUP COUNT: %s" % len(susp_mer_group_list))

    logger.info("[8]. FOR EACH GROUP, CALCULATE Tanimoto Coefficient:")
    for i in range(0, len(susp_mer_group_list)):
        if i % 500 == 0:
            logger.info("%s GROUPS FINISHED..." % i)

        # inter section count
        mer_a = susp_mer_group_list[i][0]
        mer_b = susp_mer_group_list[i][1]
        mer_a_lbl_list = pd.Series(susp_phone_trx[susp_phone_trx["mer_id"] == mer_a][parm_label]).tolist()
        mer_b_lbl_list = pd.Series(susp_phone_trx[susp_phone_trx["mer_id"] == mer_b][parm_label]).tolist()
        inter_phone_cnt = len(list(set(mer_a_lbl_list).intersection(set(mer_b_lbl_list))))

        # union section count
        if inter_phone_cnt != 0:
            mer_a_tot_lbl = mer_phone_cnt[mer_phone_cnt["mer_id"] == mer_a][parm_label].values
            mer_b_tot_lbl = mer_phone_cnt[mer_phone_cnt["mer_id"] == mer_b][parm_label].values
            union_phone_cnt = mer_a_tot_lbl[0] + mer_b_tot_lbl[0] - inter_phone_cnt

            # Calculation Tanimoto Coefficient
            tc = round(inter_phone_cnt / union_phone_cnt, 4)

            # Collect Tanimoto Coefficient
            group_tc_df = pd.DataFrame({"mer_a": [mer_a],
                                        "mer_b": [mer_b],
                                        "mer_a_cnt": [mer_a_tot_lbl[0]],
                                        "mer_b_cnt": [mer_b_tot_lbl[0]],
                                        "ic": [inter_phone_cnt],
                                        "uc": [union_phone_cnt],
                                        "tc": [tc]})
            tc_rst = tc_rst.append(group_tc_df, ignore_index=True)
            del group_tc_df
    del mer_phone_cnt
    del susp_phone_trx
    logger.info("****MERCHANT SIMILARITY ANALYSIS DATA PRE-PROCESSING END****")
    return df_dropna, tc_rst


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
        # df_trx_detail = get_trx_detail(reader)
        df_trx_detail = get_trx_detail_new(reader)
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


def crt_graph(node_1, mode_2, relation_nm, parm_df):
    logger.info("##########")
    logger.info("Start making graph...")
    for i in parm_df.index:
        create_node(node_1, mode_2, relation_nm, parm_df.loc[i].values)
    logger.info("Making graph successfully...Please check web...")
    logger.info("##########")


def crt_mer_relt_csv(parm_df):
    logger.info("\n")
    logger.info("****CREATE MERCHANT RELATION CSV FILE BEGIN****")

    logger.info("[1]. GET ALL MERCHANT RELATIONS:")
    mer_relt_df = pd.DataFrame(parm_df.loc[:, ["mer_a", "mer_b", "tc"]])
    logger.info("==>> ALL MERCHANT RELATIONS DATA FRAME SHAPE:")
    logger.info(mer_relt_df.shape)

    logger.info("[2]. FIND THE THREE CLOSEST MERCHANTS TO OTHERS:")
    tot_mer_list = pd.Series(mer_relt_df["mer_a"]).tolist() + pd.Series(mer_relt_df["mer_b"]).tolist()
    tot_mer = set(tot_mer_list)
    mer_relt_cnt = pd.DataFrame({})
    for single_mer in tot_mer:
        df = pd.DataFrame({"mer_id": [single_mer],
                           "cnt": [tot_mer_list.count(single_mer)]})
        mer_relt_cnt = mer_relt_cnt.append(df, ignore_index=True)
    mer_relt_cnt.sort_values(by="cnt", ascending=False, inplace=True)
    logger.info("==>> MERCHANT RELATIONS COUNT DF[TOP5]:\n %s" % mer_relt_cnt.head())
    sus_mer_hurdle = 5
    logger.info("==>> MERCHANT RELATIONS COUNT TOP %s == SUSPICIOUS MERCHANT" % sus_mer_hurdle)
    top_relt_mer_list = pd.Series(mer_relt_cnt.head(sus_mer_hurdle)["mer_id"]).tolist()
    logger.info("==>> SUSPICIOUS MERCHANT: %s" % top_relt_mer_list)

    logger.info("[3]. GET THE Tanimoto Coefficient BETWEEN SUSPICIOUS MERCHANTS:")
    mer_relt_rst = pd.DataFrame(mer_relt_df[mer_relt_df["mer_a"].isin(top_relt_mer_list)])\
        .append(pd.DataFrame(mer_relt_df[mer_relt_df["mer_b"].isin(top_relt_mer_list)])).reset_index(drop=True)
    mer_relt_rst.sort_values(by="tc", ascending=False, inplace=True)
    logger.info("==>> SUSPICIOUS MERCHANTS'S TOP5 Tanimoto Coefficient:\n %s " % mer_relt_rst.head())

    logger.info("[4]. SAVE THE RELATION BETWEEN SUSPICIOUS MERCHANTS:")
    sus_mer_path = conf.RESULT_PATH + "sus_mer_relation.csv"
    logger.info("SAVE TO: %s" % sus_mer_path)
    mer_relt_rst.to_csv(sus_mer_path, index=False)
    logger.info("****CREATE MERCHANT RELATION CSV FILE END****")


def crt_graph_preproc(parm_tc_rst, df_dropdup, parm_label):
    """
        Description:
        MAKING GRAPH IN SUSPICIOUS MERCHANT & PHONE

        Parameters:
          :param parm_tc_rst: 含有tc系数的df
          :param df_dropdup: 去掉空值维度的交易流水df
          :param parm_label: 维度标签（手机号、身份证、IP地址、设备指纹）

        Process:
          
        1.根据TC的额定值，找到可疑商户组，即：TC值大于额定值的商户组，定义为可疑商户组
        2.拆分商户组，找到可疑商户
          Return: susp_mer_list
        3.找到可疑商户交易流水，包含手机号
          Return: susp_mer_trx
        4.找到可疑商户，不同手机号的交易笔数
          Return: susp_mer_trx_weight
        5.在(susp_mer_trx_weight)中找到可疑手机号，即：1个手机号在2个或2个以上的可疑商户中交易
          Return: susp_phone_with_susp_mer
        6.在(susp_mer_trx_weight)中筛选可疑手机号(susp_phone_with_susp_mer)的交易，后续绘图
          Return: graph_df
        
        Returns:
          None

        Raises:
          IOError: An error occurred accessing the bigtable.Table object.
    """
    logger.info("\n")
    logger.info("****MAKING SUSPICIOUS MERCHANT & DIMENSION RELATIONS GRAPH BEGIN****")

    logger.info("[1]. FIND SUSPICIOUS GROUP WHERE TC_RATIO OVER %s:" % "10%")
    tc_rst_over_hurdle = pd.DataFrame(parm_tc_rst.loc[:, ])
    tc_rst_over_hurdle = tc_rst_over_hurdle[tc_rst_over_hurdle["tc"] > 0.1]
    tc_rst_over_hurdle.sort_values(by="tc", ascending=False, inplace=True)
    logger.info("==>> SUSPICIOUS GROUP COUNT: %s " % len(tc_rst_over_hurdle))
    # print("tc_rst_over_hurdle \n", tc_rst_over_hurdle)

    logger.info("[2]. GET SUSPICIOUS MERCHANT LIST:")
    susp_mer_list = get_susp_mer_dimen(tc_rst_over_hurdle)
    logger.info("==>> SUSPICIOUS MERCHANT LIST: %s" % susp_mer_list)
    # print("susp_mer_list \n", susp_mer_list)

    logger.info("[3]. GET SUSPICIOUS MERCHANT TRANSACTIONS WITH DIFFERENT DIMENSION:")
    susp_mer_trx = pd.DataFrame(df_dropdup[df_dropdup["mer_id"].isin(susp_mer_list)])
    logger.info("==>> SUSPICIOUS MERCHANT TRANSACTIONS COUNT: %s " % len(susp_mer_trx))

    logger.info("[4]. GET WEIGHT OF %s IN SUSPICIOUS MERCHANT: " % parm_label)
    susp_mer_trx_weight = pd.pivot_table(susp_mer_trx, index=["mer_id", parm_label], values=["inst_id"], aggfunc=len)\
        .reset_index()
    susp_mer_trx_weight.rename(columns={'inst_id': 'weight'}, inplace=True)
    logger.info("==>> SUSPICIOUS MERCHANT TRANSACTION %s WEIGHT: %s" % (parm_label, len(susp_mer_trx_weight)))

    logger.info("[5]. FIND SUSPICIOUS PHONE IN SUSPICIOUS MERCHANT:")
    # itmd_df: intermediate Dataframe
    itmd_df = pd.pivot_table(susp_mer_trx_weight, index=[parm_label], values=["mer_id"], aggfunc=len).reset_index()
    susp_phone_with_susp_mer = pd.Series(itmd_df[itmd_df["mer_id"] > 1][parm_label]).tolist()
    logger.info("==>> SUSPICIOUS PHONE LIST: %s" % susp_phone_with_susp_mer)

    logger.info("[6]. CREATE RELATION GRAPH:")
    graph_df = pd.DataFrame(susp_mer_trx_weight[susp_mer_trx_weight[parm_label].isin(susp_phone_with_susp_mer)])
    logger.info("==>> SUSPICIOUS MERCHANT & PHONE TRANSACTION COUNT: %s" % len(graph_df))
    crt_graph("MERCHANT", parm_label, "TRX_WITH", graph_df)
    logger.info("****MAKING SUSPICIOUS MERCHANT & DIMENSION RELATIONS GRAPH END****")


def create_rpt(parm_rst):
    if len(parm_rst) != 0:
        logger.info("--------------------------------------------")
        # 1. OVERALL REPORT
        # overall_rpt(parm_rst)
        logger.info("--------------------------------------------")
        # 2. MAKING RELATIONS GRAPH
        di_list = ["mobile_no"]
        # di_list = ["mobile_no", "id_no"]
        # di_list = ["mobile_no", "id_no", "td_device", "pay_ip"]
        ori_field = ["inst_id", "mer_id"]

        # logger.info(parm_rst[parm_rst["mer_id"] == "8139"])
        # logger.info(parm_rst[parm_rst["mer_id"] == "9996"])
        # parm_rst = parm_rst[parm_rst["mer_id"].isin(["8139","9996"])]
        # parm_rst = pd.DataFrame(parm_rst.loc[:, ["inst_id", "mobile_no", "mer_id"]])
        parm_rst = pd.DataFrame(parm_rst.loc[:, ori_field + di_list])
        for i in range(0, len(di_list)):
            relt_dimen = di_list[i]
            if di_list[i] == "mobile_no":
                parm_rst = parm_rst[parm_rst["mobile_no"] != "00000000000"]
                (df_dropdup, tc_rst) = mer_sim_anls_preproc(parm_rst, di_list[i])
            else:
                (df_dropdup, tc_rst) = mer_sim_anls_preproc(parm_rst, di_list[i])
            # 2.1 MAKING MERCHANT & DIMENSION RELATIONS GRAPH
            if relt_dimen != "":
                crt_graph_preproc(tc_rst, df_dropdup, relt_dimen)
        # 2.2 MAKING MERCHANT & MERCHANT RELATIONS CSV FILES
        # crt_mer_relt_csv(tc_rst)
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
    conn_neo4j()


if __name__ == '__main__':
    # global g
    # import mer_sim_analysis
    # print(help(mer_sim_analysis))
    start_time = datetime.datetime.now()
    conf = config.DeviceConfig()
    logger = Logger(path=conf.LOG_PATH)
    df_dropdup = pd.DataFrame({})
    init()
    csv_folder = get_csv_folder()
    logger.info('CSV FILES FOLDER: %s' % csv_folder)
    parm_rst = pd.DataFrame({})
    # get_relation_csv(parm_rst, "mobile_no")
    main_process(csv_folder)
    end_time = datetime.datetime.now()
    logger.info("--------------------------------------------")
    logger.info("Total Processing Time:")
    logger.info('START TIME = %s' % start_time)
    logger.info('END TIME = %s' % end_time)
    logger.info('DIFF TIME = %s' % (end_time - start_time))
    logger.info('System Processing Have Done...')
