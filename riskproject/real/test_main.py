# coding:utf-8
# -*- coding: utf-8 -*-

import datetime
import os
import shutil
import sys
import time
import tkinter.filedialog as tf
import tkinter.messagebox as tm
import traceback

import sqlalchemy as sq

import docs.conf.conf as conf
import riskproject.real.ds_stat as ds_stat


def get_csv_floder():
    global csv_biz_type
    father_path = os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep + ".")
    default_dir = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\csvfiles\\'
    # file_path = tf.askopenfilename(title=u"选择文件CSV文件", filetypes=[("csv files", "*.csv"), ("all files", "*")],
    #                                initialdir=(os.path.expanduser(default_dir)),
    #                                initialfile='')
    # csv_floder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=(os.path.expanduser(default_dir)))
    csv_floder = "E:\myself\VBA\csv_files\source\ds\0427-0503"
    if len(csv_floder) != 0:
        csv_biz_type = csv_floder.split('/')[-1]
        print('csv_biz_type = ', csv_biz_type)
    else:
        tm.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)

    return csv_floder


def init():
    pass


def insert_db(parm_df, parm_table):
    print(common_log() + '-->>' + 'original parm_df size ==>> \n ', parm_df.iloc[:, 0].size)
    print(common_log() + '-->>' + 'target table name = ', parm_table)
    engine = sq.create_engine('mysql+pymysql://root:root@127.0.0.1:3306/rcdb?charset=utf8',
                              echo=True,
                              encoding='utf-8')
    try:
        parm_df.to_sql(name=parm_table, con=engine, if_exists='append', index=False, index_label=False)
        print('DataBase Processing Success...')
    except:
        print("DataBase Processing Error...")
        tm.showinfo(title='错误提示', message='数据库操作出现错误')
        traceback.print_exc()
        sys.exit(0)


def common_log():
    return os.path.basename(__file__)


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
        print("original csv files path =", csv_file_path)
        print("move to ==>>", new_file_path)
        shutil.copy(csv_file_path, new_floder)
        os.remove(csv_file_path)

    # 3、将csvfiles_bak文件夹下对应文件夹打zip包放入/docs/bak下
    print("ready to zip csv floder ==>> ", new_floder)
    print("target zip file path ==>> ", zip_path)
    print("bak_floder ==>> ", bak_floder)
    shutil.make_archive(zip_path, 'zip', root_dir=new_floder)

    # 4、移除刚刚新建的csvfiles_bak文件夹（备份文件已经打包放入bak下，过渡文件可以删除）
    shutil.rmtree(bak_floder)
    print('Backup CSV Files Processing Done...')


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
    csv_file_list = ["1"]
    # print('csv_file_list = ', csv_file_list)
    csv_biz_type = "ds_stat"
    if csv_biz_type == conf.CS:  # cs_call
        # cs_call_rst = cs_call.get_cs_call(csv_floder, csv_file_list)
        # insert_db(cs_call_rst, 'cs_call')
        backup_csv(csv_floder, csv_file_list, "cs_call")
    elif csv_biz_type == conf.CW:  # cs_warnmgt
        # cs_warnmgt_rst = cs_warn_mgt.get_cs_warn_mgt(csv_floder, csv_file_list)
        # insert_db(cs_warnmgt_rst, 'cs_warn_mgt')
        backup_csv(csv_floder, csv_file_list, "cs_warn_mgt")
    elif csv_biz_type == conf.DS:  # td_cust_trx_hist & ds_cap_rate & ds_recg_rate
        # trx_rst = td_cust_trx.get_cust_trx_hist(csv_floder, csv_file_list)
        cap_rst = ds_stat.get_ds_cap_rate(csv_floder, csv_file_list)
        recg_rst = ds_stat.get_ds_recg_rate(csv_floder, csv_file_list)
        print("cap_rst#############\n",cap_rst)
        print("recg_rst###########\n",recg_rst)
        # insert_db(trx_rst, 'td_cust_trx_hist')
        # insert_db(cap_rst, 'ds_cap_rate')
        # insert_db(recg_rst, 'ds_recg_rate')
        # backup_csv(csv_floder, csv_file_list, "ds_stat")
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
