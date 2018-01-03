# coding:utf-8
# -*- coding: utf-8 -*-

import csv
import datetime
import os
import sys
import pandas as pd
import tkinter.filedialog as tf
import tkinter.messagebox
import traceback
import sqlalchemy as sq

import cs_call
import cs_warn_mgt
import ds_stat
import td_cust_trx
import docs.conf.conf as conf


def get_csv_dict(parm_path):
    return csv.DictReader(open(parm_path, 'r'))


def data_process(parm_csv_floder):
    """
    Data Preparation From CSV File & Insert into DB

    Parameters:
      parm_csv_floder: CSV file path

    Returns:
      None

    Raises:
      IOError: An error occurred accessing the bigtable.Table object.
    """
    if csv_biz_type == conf.CS:  # cs_call
        rst = cs_call.get_cs_call(get_csv_dict(parm_csv_floder))
        insert_db(rst, 'cs_call')
    elif csv_biz_type == conf.CW:  # cs_warn_mgt
        rst = cs_warn_mgt.get_cs_warn_mgt(get_csv_dict(parm_csv_floder))
        insert_db(rst, 'cs_warn_mgt')
    elif csv_biz_type == conf.DS:  # rc_trx_detail & ds_cap_rate & ds_recg_rate
        trx_rst = td_cust_trx.get_cust_trx_hist(get_csv_dict(parm_csv_floder))
        cap_rst = ds_stat.get_ds_cap_rate(get_csv_dict(parm_csv_floder))
        recg_rst = ds_stat.get_ds_recg_rate(get_csv_dict(parm_csv_floder))

        # trx_rst_append = trx_rst_append.append(trx_rst)
        # cap_rst_append = cap_rst_append.append(cap_rst)
        # recg_rst_append = recg_rst_append.append(recg_rst)
        # print('trx_rst_append\n',trx_rst_append)
        # print('cap_rst_append\n', cap_rst_append)
        # print('recg_rst_append\n', recg_rst_append)

        insert_db(trx_rst, 'td_cust_trx_hist')
        insert_db(cap_rst, 'ds_cap_rate')
        insert_db(recg_rst, 'ds_recg_rate')
    else:
        tkinter.messagebox.showinfo(title='提示', message='处理类型有误，请重新选择')
        print('csv_biz_type error -->> ', csv_biz_type)
        sys.exit(0)




def get_csv_floder():
    global csv_biz_type
    father_path = os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep + ".")
    default_dir = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\csvfiles\\'
    # file_path = tf.askopenfilename(title=u"选择文件CSV文件", filetypes=[("csv files", "*.csv"), ("all files", "*")],
    #                                initialdir=(os.path.expanduser(default_dir)),
    #                                initialfile='')
    csv_floder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=(os.path.expanduser(default_dir)))
    if len(csv_floder) != 0:
        # csv_file_nm = file_path.split('/')[-1]
        csv_biz_type = csv_floder.split('/')[-1]
        print('csv_biz_type = ', csv_biz_type)
    else:
        tkinter.messagebox.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)

    return csv_floder


def init():
    pass


def insert_db(parm_df, parm_table):
    print(common_log() + '-->>' + 'original parm_df ==>> \n ', parm_df)
    print(common_log() + '-->>' + 'target table name = ', parm_table)
    engine = sq.create_engine('mysql+pymysql://root:root@127.0.0.1:3306/rcdb?charset=utf8', echo=True, encoding='utf-8')
    try:
        parm_df.to_sql(name=parm_table, con=engine, if_exists='append', index=False, index_label=False)
        print('Success...')
    except:
        print("Error...")
        traceback.print_exc()


def common_log():
    return os.path.basename(__file__)


def main_process(parm_csv_floder):
    csv_file_list = os.listdir(parm_csv_floder)
    print('csv_file_list = ', csv_file_list)
    for csv_file in csv_file_list:
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_floder, '/', csv_file))
        print('csv_file_path =', csv_file_path)
        data_process(csv_file_path)
    print('Main Processing Have Done...')


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    csv_floder = get_csv_floder()
    print('csv_floder = ', csv_floder)
    main_process(csv_floder)
    end_time = datetime.datetime.now()
    print('start_time =', start_time)
    print('end_time   =', end_time)
    print('diff_Time =', end_time - start_time)
    print('System Processing Done...')
