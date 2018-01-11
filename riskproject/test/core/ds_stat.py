import pandas as pd
import time
import os
import sys
import csv


def get_prod_id(row):
    a = row["支付产品编号"]
    if pd.isnull(a):
        pass
    elif len(a) > 8:
        return a[0:8]
    else:
        print("lalala",a)
        pass
    
    
def get_csv_dict(parm_path):
    return csv.DictReader(open(parm_path, "r"))


def get_trx_list(parm_csv_reader):
    df_all_trx = pd.DataFrame({})
    df_td_cap_trx = pd.DataFrame({})
    df_zy_cap_trx = pd.DataFrame({})
    df_all_trx_grouped = pd.DataFrame({})
    df_td_cap_trx_grouped = pd.DataFrame({})
    df_zy_cap_trx_grouped = pd.DataFrame({})
    for line in parm_csv_reader:
        # print("line \n",line)
        if 1 < parm_csv_reader.line_num < 30000:
            plat_date = line["平台日期"].replace("-", "")
            inst_trace = line["机构请求流水"]
            prod_id = line["支付产品编号"][0:8]
            if len(prod_id) == 8 and "" != plat_date:
                # 1.Count All Transaction & Group By
                all_trx = pd.DataFrame({"plat_date": [plat_date], "inst_trace": [inst_trace], "prod_id": [prod_id]})
                df_all_trx = df_all_trx.append(all_trx)
                df_all_trx_grouped = df_all_trx.groupby(
                    [df_all_trx["plat_date"], df_all_trx["prod_id"]]).size().reset_index()

                # 2.Count Transaction which TD_Device Capture & Group By
                if line["同盾设备指纹"] != "" and line["同盾设备指纹"] != "111111":
                    td_cap_trx = pd.DataFrame(
                        {"plat_date": [plat_date], "inst_trace": [inst_trace], "prod_id": [prod_id]})
                    df_td_cap_trx = df_td_cap_trx.append(td_cap_trx)
                    df_td_cap_trx_grouped = df_td_cap_trx.groupby(
                        [df_td_cap_trx["plat_date"], df_td_cap_trx["prod_id"]]).size().reset_index()
                # 3.Count Transaction which ZY_Device Capture & Group By
                if line["自研设备指纹"] != "" and line["自研设备指纹"] != "000000":
                    zy_cap_trx = pd.DataFrame(
                        {"plat_date": [plat_date], "inst_trace": [inst_trace], "prod_id": [prod_id]})
                    df_zy_cap_trx = df_zy_cap_trx.append(zy_cap_trx)
                    df_zy_cap_trx_grouped = df_zy_cap_trx.groupby(
                        [df_zy_cap_trx["plat_date"], df_zy_cap_trx["prod_id"]]).size().reset_index()

    return df_all_trx_grouped, df_td_cap_trx_grouped, df_zy_cap_trx_grouped


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
    merge_td = pd.merge(trx_cnt, td_cap_cnt, how="left", on=["plat_date", "prod_id"])
    merge_zy = pd.merge(merge_td, zy_cap_cnt, how="left", on=["plat_date", "prod_id"])
    merge_zy.rename(columns={"td_device": "td_cap_cnt", "zy_device": "zy_cap_cnt"}, inplace=True)
    return merge_zy


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
    print("rst \n", rst)
    return rst


def get_trx_list2(parm_csv_reader):
    df_filtered_trx = pd.DataFrame({})
    for line in parm_csv_reader:
        # if 1 < parm_csv_reader.line_num < 100000:
            inst_trace = line["机构请求流水"]
            plat_date = line["平台日期"].replace("-", "")
            td_device = line["同盾设备指纹"]
            zy_device = line["自研设备指纹"]
            prod_id = line["支付产品编号"][0:8]
            if "" != plat_date:
                # Filter Transaction which has Both TD & ZY Device Fingerprint
                if td_device != "" and td_device != "111111" \
                        and zy_device != "" and zy_device != "000000":
                    filtered_trx = pd.DataFrame({"plat_date": [plat_date], "prod_id": [prod_id],
                                                 "inst_trace": inst_trace,
                                                 "td_device": [td_device], "zy_device": [zy_device]})
                    df_filtered_trx = df_filtered_trx.append(filtered_trx)
    print("df_filtered_trx \n", df_filtered_trx)
    print("df_filtered_trx =", df_filtered_trx.iloc[:, 0].size)
    return df_filtered_trx


def get_ds_recg_rate(parm_csv_floder, parm_csv_file_list):
    rst = pd.DataFrame({})
    for csv_file in parm_csv_file_list:
        # 1. Get csv files path by csv floder & csv file list
        csv_file_path = os.path.join("%s%s%s" % (parm_csv_floder, "/", csv_file))
        print("csv_file_path =", csv_file_path)

        # 2. Get csv dict by csv file path & Process Csv Dict to Get df_all_trx_grouped
        csv_reader = get_csv_dict(csv_file_path)
        df_filtered_trx = get_trx_list2(csv_reader)



        # 3. Get PRODUCT_ID, START_DATE, END_DATE
        rtn_prod_id = df_filtered_trx.iloc[0]["prod_id"]
        df_filtered_trx_sort = df_filtered_trx.sort_values(by=["plat_date"], ascending=[True])
        # print("df_filtered_trx_sort\n", df_filtered_trx_sort)
        start_date = df_filtered_trx_sort.iloc[0]["plat_date"]
        end_date = df_filtered_trx_sort.iloc[-1]["plat_date"]
        rtn_date_range = start_date + "-" + end_date
        print("rtn_prod_id = ", rtn_prod_id)
        print("rtn_date_range = ", rtn_date_range)

        # 4. Find Duplicated ZY_Device ==>> dup_zy_di
        df_filtered_trx_grouped = df_filtered_trx.groupby(
            [df_filtered_trx["prod_id"], df_filtered_trx["zy_device"]]).size().reset_index()
        df_filtered_trx_grouped.columns = ["prod_id", "zy_device", "zyCapCnt"]
        # print("df_filtered_trx_grouped\n", df_filtered_trx_grouped)
        dup_zy_di = df_filtered_trx_grouped[df_filtered_trx_grouped["zyCapCnt"] > 1]["zy_device"].reset_index()
        dup_zy_di.columns = ["index", "dup_zy_device"]

        # 5. Count Duplicated ZY_Device ==>> dup_zy_di_cnt
        dup_zy_di_cnt = len(dup_zy_di)

        # 6. Find TD_Device By Duplicated ZY_Device ==>> td_di
        td_di = pd.merge(dup_zy_di[["dup_zy_device"]],
                         df_filtered_trx[["td_device", "zy_device"]],
                         how="inner",
                         left_on="dup_zy_device",
                         right_on="zy_device")

        # 7. Count No-Duplicated TD_Device ==>> nodup_td_di_cnt
        nodup_td_di_cnt = len(td_di.drop_duplicates(["td_device"]))

        # 8. Concatenate Return DataDataFrame ==>> all_trx
        # print("dup_zy_di_cnt = ", dup_zy_di_cnt)
        # print("nodup_td_di_cnt = ", nodup_td_di_cnt)

        all_trx = pd.DataFrame({"date_range": [rtn_date_range],
                               "prod_id": [rtn_prod_id],
                               "zy_recg_cnt": [dup_zy_di_cnt],
                               "td_recg_cnt": [nodup_td_di_cnt],
                               "batch_no": time.strftime("%Y%m%d%H%M%S")}, index=None)

        rst = rst.append(all_trx, ignore_index=True)
    #     print("rst \n", rst)
    #     print("for rst =", rst.iloc[:, 0].size)
    # print("rst =", rst.iloc[:, 0].size)
    return rst

