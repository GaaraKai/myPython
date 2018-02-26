import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os
import sys
import tkinter.filedialog as tf
import tkinter.messagebox as tm
import traceback
import sqlalchemy as sq

import shutil
import time


def get_trx_detail(parm_reader):
    rtn_df = pd.DataFrame({})
    for chunk in parm_reader:
        rtn_df = rtn_df.append(chunk)
        # print("rtn_df.shape = ", rtn_df.shape)
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
    # print(rtn_df.tail())
    # print("ALL rtn_df.shape = ", rtn_df.shape)

    return rtn_df


def get_device_trx_hist(parm_csv_floder, parm_csv_file_list):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name, sys._getframe().f_lineno)
    rst = pd.DataFrame({})
    mean_list = []
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_floder, '/', csv_file))
        print('csv_file_path =', csv_file_path)
        # csv_file_path = "D:/github_program/myPython/docs/csvfiles/todo/NO 1_td_1"


        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding='utf-8', chunksize=5000, iterator=True, sep="|",dtype=str)

        # 3. 数据处理
        df_trx_detail = get_trx_detail(reader)
        # mean = int(df_trx_detail["trx_amount"].mean())
        print("df_trx_detail =",df_trx_detail.shape)
        # print("trx amt mean =", mean)
        # print(df_trx_detail)
        # mean_list.append(mean)

        df_trx_detail = df_trx_detail.dropna(axis=0, subset=["mobile_no", "prod_id", "id_no"])
        df_trx_detail = df_trx_detail[df_trx_detail["mobile_no"] != "00000000000"]

        rst = rst.append(df_trx_detail, ignore_index=True)
        reader.close()

    print("////////////////////////////////////////////")
    # print(rst.head())
    # rst = rst.dropna(axis=0, how="any")
    print(rst.shape)
    # 找到存在空值的列
    print(rst.isnull().any())
    # 找到存在空值的行
    print(rst[rst.isnull().values==True])
    # print(rst["prod_id"].unique())
    # rst = rst.dropna(axis=0, subset=["mobile_no","prod_id","id_no"])
    # rst = rst[rst["mobile_no"] != "00000000000"]
    # print(rst)
    # print(rst)
    # print(rst.describe())
    # print(rst["prod_id"].unique())

    """
    sys.exit(0)
    print("////////////////////////////////////////////")
    amt_list = rst["trx_amount"]
    # print("mean_list ", mean_list)
    # total_mean = sum(mean_list) / len(mean_list)
    # print("total trx amt mean = ",total_mean)

    mean = int(rst["trx_amount"].mean())
    print("total trx amt mean = ", mean)
    print("***********************************************")
    """
    """
    rlat_mobile_no = pd.pivot_table(rst, index=["td_device"], values="mobile_no", aggfunc=len)
    rlat_mobile_no = rlat_mobile_no.reset_index()
    print(rlat_mobile_no.shape)
    print(rlat_mobile_no.head())
    aa = rlat_mobile_no[rlat_mobile_no["mobile_no"] == 1]
    print(aa.shape)
    print(aa.head())
    """
    """
    rlat_mer_id= pd.pivot_table(rst, index=["td_device,mer_id"], values="inst_id", aggfunc=len)
    rlat_mer_id = rlat_mer_id.reset_index()
    ee = rlat_mer_id.sort_values(by="inst_id", axis=0)
    print(rlat_mer_id.shape)
    print(ee)



    sys.exit(0)
    print("***********************************************")

    fig = plt.figure(figsize=(20, 20))
    ax_amount = fig.add_subplot(1, 1, 1)
    # ax2 = fig.add_subplot(2, 1, 2)
    ax_amount.hist(amt_list, range=(0, mean), bins=6)
    ax_amount.set_title('Distribution of Transaction Amount')
    plt.legend()
    # plt.show()


    print("////////////////////////////////////////////")
    """


    """
    sys.exit(0)
    print("*********************************************")
    # print(df_trx_detail["trx_amount"])


    x = df_trx_detail["prod_id"].value_counts()
    # print(x)
    print("*********************************************")
    b = pd.pivot_table(df_trx_detail, index=["prod_id"], values="inst_id", aggfunc=len)
    # print(type(b))
    b = b.reset_index()
    # print(b)
    num_cols = b["prod_id"]
    num_cols = num_cols.tolist()
    # print(num_cols)

    c = b["inst_id"]
    c = c.tolist()
    # print(c)

    # 3
    # xx = pd.pivot_table(df_trx_detail, index=["td_device","prod_id"], values=["inst_id"], aggfunc=len)
    # xx = xx.reset_index()
    # print(xx.shape)
    # yy = xx[xx["inst_id"] >= 50] #ffa4a48d333b4d0baf824beb6bfe9af3
    # z = yy.sort_values(by="inst_id", axis=0, ascending=True)
    # print(z)

    fig = plt.figure(figsize=(20, 20))
    ax1 = fig.add_subplot(2, 1, 1)
    ax2 = fig.add_subplot(2, 1, 2)
    # ax3 = fig.add_subplot(4, 1, 3)
    # ax4 = fig.add_subplot(4, 1, 4)

    ax1.hist(a, range=(0, 5000), bins=20)
    ax1.set_title('Distribution of Transaction Amount')
    ax2.bar(num_cols, c, 0.4, color="green")
    ax2.set_title('b')
    # ax3.bar(yy["td_device"],yy["inst_id"],color="red")
    # ax3.set_title('3')
    plt.show()
    print("*********************************************")

    sys.exit(0)
    print('rst /n', rst.tail())
    """
    return rst

def get_csv_floder():
    global csv_biz_type
    father_path = os.path.abspath(os.path.dirname(os.getcwd()) + os.path.sep + ".")
    default_dir = os.path.abspath(os.path.dirname(father_path) + os.path.sep + "..") + '\\docs\\csvfiles\\'
    # csv_floder = "D:/github_program/myPython/docs/csvfiles/todo/"
    csv_floder = tf.askdirectory(title=u"选择文件CSV文件夹", initialdir=(os.path.expanduser(default_dir)))
    if len(csv_floder) != 0:
        csv_biz_type = csv_floder.split('/')[-1]
        print('csv_biz_type = ', csv_biz_type)
    else:
        tm.showinfo(title='提示', message='请选取的CSV文件夹')
        sys.exit(0)

    return csv_floder


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
    csv_file_list = os.listdir(parm_csv_floder)
    print('csv_file_list = ', csv_file_list)
    get_device_trx_hist(csv_floder, csv_file_list)
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



