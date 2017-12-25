#!/usr/bin/python3
# coding:utf8
# -*- coding: utf-8 -*-
# Read CSV file(cs_warnmgt) and Insert to MySQL table (cw_warnmgt)
import csv
import os
import pymysql
import traceback
import time
import pandas as pd
import sqlalchemy as sq
import datetime


def csv_reader(csv_path):
    df_all_trx = pd.DataFrame({})
    with open(csv_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        # print(1)
        for line in csv_reader:
            # if 1< csv_reader.line_num<30:
                # print(csv_reader.fieldnames)
                insert_value = get_value(line)
                df_all_trx = df_all_trx.append(insert_value)
        df_all_trx.reset_index()
    # print('df_all_trx\n',df_all_trx)
    return df_all_trx


def get_value(line):
    rule_no = line[0].split(':')[0]
    stat_count = line[1]
    batch_no = time.strftime("%Y%m%d%H%M%S")
    # print('risk_no = ',risk_no,'mer_id = ',mer_id,'mer_name = ',mer_name)
    insert_value = pd.DataFrame({'rule_no': [rule_no], 'stat_count': [stat_count],'batch_no': [batch_no]})
    return insert_value


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    # 当前文件的路径
    file_path = os.getcwd()
    csv_path = r'E:\SVN文档数据\08_产品风控\11.大型项目\05.自研风控核心\验收对比\csv\ZY_1224.csv'
    class_type = csv_path.split('\\')[-1].split('.')[0].split('_')[0]
    print('class_type = ',class_type)
    date = '2017'+csv_path.split('\\')[-1].split('.')[0].split('_')[-1]
    print('date = ',date)
    df_insert = pd.DataFrame({})
    df_insert = csv_reader(csv_path)
    df_insert['date'] = date
    print('df_insert\n',df_insert)
    if class_type == 'BS':
        tbl_name = 'bs_rule_stat'
    elif class_type == 'ZY':
        tbl_name = 'zy_rule_stat'
    print('tbl_name = ',tbl_name)

    # insert to DB
    engine = sq.create_engine('mysql+pymysql://root:root@127.0.0.1:3306/rcdb?charset=utf8', \
                              echo=True, encoding='utf-8')
    df_insert.to_sql(name=tbl_name, con=engine, if_exists='append', index=False, index_label=False)
    end_time = datetime.datetime.now()

    # # 当前文件的路径
    # pwd = os.getcwd()
    # # 当前文件的父路径
    # father_path = os.path.abspath(os.path.dirname(pwd) + os.path.sep + ".")
    # # 当前文件的前两级目录
    # grader_father = os.path.abspath(os.path.dirname(pwd) + os.path.sep + "..")
    # # print('pwd = ',pwd)
    # # print('father_path = ', father_path)
    # # print('grader_father = ', grader_father)
    # print('start_time =', start_time)
    # print('end_time   =', end_time)
    # print('diff_Time = ', end_time - start_time)

