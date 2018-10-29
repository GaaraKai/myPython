import pandas as pd
import time
import os
import sys


def get_prod_id(row):
    a = row["支付产品编号"]
    if pd.isnull(a):
        pass
    elif len(a) > 8:
        return a[0:8]
    else:
        print("lalala",a)
        pass


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
    # print("trx_cnt\n",trx_cnt)
    # 1. 排除异常数据（空值，111111,000000）
    all_trx_dropna = all_trx.dropna(axis=0, how="any")
    all_trx_drop11 = all_trx_dropna[all_trx_dropna["td_device"] != "111111"]
    all_trx_drop00 = all_trx_dropna[all_trx_dropna["zy_device"] != "000000"]
    # print("all_trx_drop11\n",all_trx_drop11)
    # print("all_trx_drop00\n", all_trx_drop00)
    # 2. 统计获取数
    td_cap_cnt = pd.pivot_table(all_trx_drop11, values="td_device", index=["plat_date", "prod_id"], aggfunc=len)
    zy_cap_cnt = pd.pivot_table(all_trx_drop00, values="zy_device", index=["plat_date", "prod_id"], aggfunc=len)
    # print("td_cap_cnt\n",td_cap_cnt)
    # print("zy_cap_cnt\n", zy_cap_cnt)

    # 3. 重置索引
    print("-------reset_index------")
    trx_cnt = trx_cnt.reset_index()
    td_cap_cnt = td_cap_cnt.reset_index()
    zy_cap_cnt = zy_cap_cnt.reset_index()

    # 4. 合并DF
    # print("trx_cnt \n",trx_cnt)
    # print("td_cap_cnt \n",td_cap_cnt)

    if len(td_cap_cnt) == 0:
        merge_td = trx_cnt
        merge_td["td_device"] = 0
        print("merge_td \n", merge_td)
    else:
        merge_td = pd.merge(trx_cnt, td_cap_cnt, how="left", on=["plat_date", "prod_id"])

    if len(zy_cap_cnt) == 0:
        merge_zy = merge_td
        merge_zy["zy_device"] = 0
        print("merge_zy \n", merge_zy)
    else:
        merge_zy = pd.merge(merge_td, zy_cap_cnt, how="left", on=["plat_date", "prod_id"])


    # merge_zy = pd.merge(merge_td, zy_cap_cnt, how="left", on=["plat_date", "prod_id"])
    merge_zy.rename(columns={"td_device": "td_cap_cnt", "zy_device": "zy_cap_cnt"}, inplace=True)
    print(sys._getframe().f_code.co_name, "merge_zy\n", merge_zy)

    return merge_zy


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
    # print("all_trx111 \n",all_trx)
    # print("len(all_trx) = ",len(all_trx))

    # 2. 获取交易ID，时间范围
    rtn_prod_id = all_trx.iloc[0]["prod_id"]
    all_trx_sort = all_trx.sort_values(by=["plat_date"], ascending=True)
    start_date = all_trx_sort.iloc[0]["plat_date"].replace("-","")
    end_date = all_trx_sort.iloc[-1]["plat_date"].replace("-","")
    rtn_date_range = start_date + "-" + end_date
    # print("rtn_prod_id = ", rtn_prod_id)
    # print("rtn_date_range = ", rtn_date_range)

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
        print("zy_recg_cnt", zy_recg_cnt)

        # 5. 找到自研认为是同一设备的设备指纹（zy_dup_list）
        trx_cnt = trx_cnt.reset_index()
        zy_dup_list = trx_cnt[["zy_device"]]

        # 6.  找到自研认为是同一设备的设备指纹所对应的同盾设备指纹，并统计其个数（td_recg_cnt）
        td_di = pd.merge(all_trx,zy_dup_list,how="inner",on="zy_device")
        td_di = td_di.drop_duplicates(["td_device"])
        td_recg_cnt = len(td_di)
        print("td_recg_cnt \n", td_recg_cnt)

        # 7.  合并DF返回
        rst = pd.DataFrame({"date_range": [rtn_date_range],
                            "prod_id": [rtn_prod_id],
                            "td_recg_cnt": [td_recg_cnt],
                            "zy_recg_cnt": [zy_recg_cnt],}, index=None)
    print(sys._getframe().f_code.co_name, "rst\n", rst)
    return rst


def get_ds_cap_rate(parm_csv_floder, parm_csv_file_list):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name, "line_no =", sys._getframe().f_lineno)
    rst = pd.DataFrame({})
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join("%s%s%s" % (parm_csv_floder, "/", csv_file))
        print("csv_file_path =", csv_file_path)

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding="gbk", chunksize=5000, iterator=True, dtype=str)

        # 3. 数据处理
        df_cap_rate = get_cap_rate(reader)
        rst = rst.append(df_cap_rate, ignore_index=True)

    # 4. 计算获取率
    rst["td_cap_rate"] = rst["td_cap_cnt"] / rst["trx_cnt"]
    rst["zy_cap_rate"] = rst["zy_cap_cnt"] / rst["trx_cnt"]
    rst["batch_no"] = time.strftime("%Y%m%d%H%M%S")
    print(sys._getframe().f_code.co_name, "rst\n", rst)
    return rst


def get_ds_recg_rate(parm_csv_floder, parm_csv_file_list):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name, "line_no =", sys._getframe().f_lineno)
    rst = pd.DataFrame({})
    for csv_file in parm_csv_file_list:
        # 1. 获取CSV文件路径
        csv_file_path = os.path.join("%s%s%s" % (parm_csv_floder, "/", csv_file))
        print("csv_file_path =", csv_file_path)

        # 2. 读取CSV文件
        reader = pd.read_csv(csv_file_path, encoding="gbk", chunksize=5000, iterator=True, dtype=str)

        # 3. 数据处理
        df_recg_rate = get_recg_rate(reader)
        rst = rst.append(df_recg_rate, ignore_index=True)
    rst["batch_no"] = time.strftime("%Y%m%d%H%M%S")
    print(sys._getframe().f_code.co_name, "rst\n", rst)
    return rst

