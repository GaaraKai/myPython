# coding:utf-8
# -*- coding: utf-8 -*-

import csv
import datetime
import os
import sys
import tkinter.filedialog as tf
import tkinter.messagebox
import traceback

import pandas as pd
import sqlalchemy as sq

import cs_call
import cs_warn_mgt
import ds_cap_rate
import ds_recg_rate


def csv_reader(parm_csv_path):
    df_to_db = pd.DataFrame({})
    with open(parm_csv_path, 'r') as opened_file:
        csv_read_result = csv.DictReader(opened_file)
        df_to_db = get_csv_value(csv_read_result)
    # print('df_to_db\n', df_to_db)
    return df_to_db


def get_csv_value(parm_csv_read_result):
    # rst = pd.DataFrame({})
    global tbl_name
    if csv_biz_type == 'cs_call':
        # cs_call
        rst = cs_call.get_cs_call(parm_csv_read_result)
        tbl_name = 'cs_call'
    elif csv_biz_type == 'cs_warnmgt':
        # cs_warn_mgt
        rst = cs_warn_mgt.get_cs_warn_mgt(parm_csv_read_result)
        tbl_name = 'cs_warn_mgt'
    elif csv_biz_type == 'ds_caprate':
        # ds_cap_rate
        rst = ds_cap_rate.get_ds_cap_rate(parm_csv_read_result)
        tbl_name = 'ds_cap_rate'
    elif csv_biz_type == 'ds_recgrate':
        # ds_recg_rate
        rst = ds_recg_rate.get_ds_recg_rate(parm_csv_read_result)
        tbl_name = 'ds_recg_rate'
    else:
        error_msg = tkinter.messagebox.showinfo(title='提示', message='处理类型有误，请重新选择')
        print('csv_biz_type error -->> ', csv_biz_type)
        sys.exit(0)
    return rst


def get_csv_path():
    global csv_biz_type
    default_dir = r"D:\github_program\myPython\docs\csvfiles"  # 设置默认打开目录
    file_path = tf.askopenfilename(title=u"选择文件CSV文件", filetypes=[("csv files", "*.csv"), ("all files", "*")],
                                   initialdir=(os.path.expanduser(default_dir)),
                                   initialfile='')
    if len(file_path) != 0:
        # csv_file_nm = file_path.split('/')[-1]
        csv_biz_type = file_path.split('/')[-2]
        print('csv_biz_type = ', csv_biz_type)
    else:
        tkinter.messagebox.showinfo(title='提示', message='请选取的CSV文件')
        sys.exit(0)

    return file_path


def init():
    pass


def insert_db(parm_df, parm_table):
    print('original parm_df ==>> \n ', parm_df)
    print('target table name = ', parm_table)
    engine = sq.create_engine('mysql+pymysql://root:root@127.0.0.1:3306/rcdb?charset=utf8', echo=True, encoding='utf-8')
    try:
        parm_df.to_sql(name=parm_table, con=engine, if_exists='append', index=False, index_label=False)
        print('Success...')
    except:
        print("Error...")
        traceback.print_exc()


def common_log():
    return os.path.basename(__file__)


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    csv_path = get_csv_path()
    print('csv_path = ', csv_path)
    rtn_df = csv_reader(csv_path)
    print(common_log() + '-->>' + 'rtn_df\n', rtn_df)
    # insert to DB
    # insert_db(rtn_df, tbl_name)
    end_time = datetime.datetime.now()
    print('start_time =', start_time)
    print('end_time   =', end_time)
    print('diff_Time =', end_time - start_time)
