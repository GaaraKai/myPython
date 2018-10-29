import pandas as pd
import time
import os
import sys
import logging
from riskproject.real import config
# from riskproject.real import logger as asd


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


def common_logger():
    conf = config.WeeklyStatConfig()
    logger = Logger(path=conf.LOG_PATH)
    return logger
# conf = config.WeeklyStatConfig()
# logger = Logger(path=conf.LOG_PATH)


def get_prod_id(row):
    logger = common_logger()
    a = row["支付产品编号"]
    if pd.isnull(a):
        pass
    elif len(a) > 8:
        return a[0:8]
    else:
        logger.info("lalala")
        pass


def get_cap_rate(parm_reader):
    logger = common_logger()
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
    # logger.info("trx_cnt\n",trx_cnt)
    # 1. 排除异常数据（空值，111111,000000）
    all_trx_dropna = all_trx.dropna(axis=0, how="any")
    all_trx_drop11 = all_trx_dropna[all_trx_dropna["td_device"] != "111111"]
    all_trx_drop00 = all_trx_dropna[all_trx_dropna["zy_device"] != "000000"]
    # logger.info("all_trx_drop11\n",all_trx_drop11)
    # logger.info("all_trx_drop00\n", all_trx_drop00)
    # 2. 统计获取数
    td_cap_cnt = pd.pivot_table(all_trx_drop11, values="td_device", index=["plat_date", "prod_id"], aggfunc=len)
    zy_cap_cnt = pd.pivot_table(all_trx_drop00, values="zy_device", index=["plat_date", "prod_id"], aggfunc=len)
    # logger.info("td_cap_cnt\n",td_cap_cnt)
    # logger.info("zy_cap_cnt\n", zy_cap_cnt)

    # 3. 重置索引
    logger.info("-------reset_index------")
    trx_cnt = trx_cnt.reset_index()
    td_cap_cnt = td_cap_cnt.reset_index()
    zy_cap_cnt = zy_cap_cnt.reset_index()

    # 4. 合并DF
    # logger.info("trx_cnt \n",trx_cnt)
    # logger.info("td_cap_cnt \n",td_cap_cnt)

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


    # merge_zy = pd.merge(merge_td, zy_cap_cnt, how="left", on=["plat_date", "prod_id"])
    merge_zy.rename(columns={"td_device": "td_cap_cnt", "zy_device": "zy_cap_cnt"}, inplace=True)

    return merge_zy


def get_ds_cap_rate(parm_csv_floder, parm_csv_file_list):
    conf = config.WeeklyStatConfig()
    logger = Logger(path=conf.LOG_PATH)
    rst = pd.DataFrame({})
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join("%s%s%s" % (parm_csv_floder, "/", csv_file))
        logger.info("csv_file_path = %s " % csv_file_path)
        csv_file_path = "E:/myself/VBA/csv_files/source\ds/0427-0503/DS_0427-0503_094955.csv"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding="gbk", chunksize=5000, iterator=True, dtype=str)

        # 3. 数据处理
        df_cap_rate = get_cap_rate(reader)
        rst = rst.append(df_cap_rate, ignore_index=True)

    # 4. 计算获取率
    rst["td_cap_rate"] = rst["td_cap_cnt"] / rst["trx_cnt"]
    rst["zy_cap_rate"] = rst["zy_cap_cnt"] / rst["trx_cnt"]
    rst["batch_no"] = time.strftime("%Y%m%d%H%M%S")
    logger.info("rst %s \n" % rst)
    return rst


def get_recg_rate(parm_reader):
    conf = config.WeeklyStatConfig()
    logger = Logger(path=conf.LOG_PATH)
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
        # logger.info("zy_recg_cnt: %s" % zy_recg_cnt)

        # 5. 找到自研认为是同一设备的设备指纹（zy_dup_list）
        trx_cnt = trx_cnt.reset_index()
        zy_dup_list = trx_cnt[["zy_device"]]

        # 6.  找到自研认为是同一设备的设备指纹所对应的同盾设备指纹，并统计其个数（td_recg_cnt）
        td_di = pd.merge(all_trx,zy_dup_list,how="inner",on="zy_device")
        td_di = td_di.drop_duplicates(["td_device"])
        td_recg_cnt = len(td_di)
        # logger.info("td_recg_cnt: %s \n" % td_recg_cnt)

        # 7.  合并DF返回
        rst = pd.DataFrame({"date_range": [rtn_date_range],
                            "prod_id": [rtn_prod_id],
                            "td_recg_cnt": [td_recg_cnt],
                            "zy_recg_cnt": [zy_recg_cnt],}, index=None)
    # logger.info("rst\n %s" % rst)
    return rst


def get_ds_recg_rate(parm_csv_floder, parm_csv_file_list):
    conf = config.WeeklyStatConfig()
    logger = Logger(path=conf.LOG_PATH)
    # print(logger)
    rst = pd.DataFrame({})
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join("%s%s%s" % (parm_csv_floder, "/", csv_file))
        # print("csv_file_path = %s" )
        # csv_file_path = "E:/myself/VBA/csv_files/source\ds/0427-0503/DS_0427-0503_094955.csv"

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding="gbk", chunksize=5000, iterator=True, dtype=str)

        # 3. 数据处理
        df_recg_rate = get_recg_rate(reader)
        rst = rst.append(df_recg_rate, ignore_index=True)
    rst["batch_no"] = time.strftime("%Y%m%d%H%M%S")
    return rst

