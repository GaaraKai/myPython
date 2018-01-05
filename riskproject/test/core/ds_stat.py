import pandas as pd
import time
import os
import sys
import csv


def get_csv_dict(parm_path):
    return csv.DictReader(open(parm_path, 'r'))


def get_trx_list(parm_csv_reader):
    df_all_trx = pd.DataFrame({})
    df_td_cap_trx = pd.DataFrame({})
    df_zy_cap_trx = pd.DataFrame({})
    df_all_trx_grouped = pd.DataFrame({})
    df_td_cap_trx_grouped = pd.DataFrame({})
    df_zy_cap_trx_grouped = pd.DataFrame({})
    for line in parm_csv_reader:
        # print('line \n',line)
        if 1 < parm_csv_reader.line_num < 30000:
            plat_date = line['平台日期'].replace('-', '')
            inst_trace = line['机构请求流水']
            prod_id = line['支付产品编号'][0:8]
            if len(prod_id) == 8 and '' != plat_date:
                # 1.Count All Transaction & Group By
                all_trx = pd.DataFrame({'plat_date': [plat_date], 'inst_trace': [inst_trace], 'prod_id': [prod_id]})
                df_all_trx = df_all_trx.append(all_trx)
                df_all_trx_grouped = df_all_trx.groupby(
                    [df_all_trx['plat_date'], df_all_trx['prod_id']]).size().reset_index()

                # 2.Count Transaction which TD_Device Capture & Group By
                if line['同盾设备指纹'] != "" and line['同盾设备指纹'] != '111111':
                    td_cap_trx = pd.DataFrame(
                        {'plat_date': [plat_date], 'inst_trace': [inst_trace], 'prod_id': [prod_id]})
                    df_td_cap_trx = df_td_cap_trx.append(td_cap_trx)
                    df_td_cap_trx_grouped = df_td_cap_trx.groupby(
                        [df_td_cap_trx['plat_date'], df_td_cap_trx['prod_id']]).size().reset_index()
                # 3.Count Transaction which ZY_Device Capture & Group By
                if line['自研设备指纹'] != "" and line['自研设备指纹'] != '000000':
                    zy_cap_trx = pd.DataFrame(
                        {'plat_date': [plat_date], 'inst_trace': [inst_trace], 'prod_id': [prod_id]})
                    df_zy_cap_trx = df_zy_cap_trx.append(zy_cap_trx)
                    df_zy_cap_trx_grouped = df_zy_cap_trx.groupby(
                        [df_zy_cap_trx['plat_date'], df_zy_cap_trx['prod_id']]).size().reset_index()

    return df_all_trx_grouped, df_td_cap_trx_grouped, df_zy_cap_trx_grouped


def get_ds_cap_rate(parm_csv_floder, parm_csv_file_list):
    # print(os.path.basename(__file__), sys._getframe().f_code.co_name,
    #       sys._getframe().f_lineno, 'csv_reader = ', csv_reader)
    rst = pd.DataFrame({})
    # print(os.path.basename(__file__), sys._getframe().f_code.co_name, 'line_no =', sys._getframe().f_lineno)
    for csv_file in parm_csv_file_list:
        # 1. Get csv files path by csv floder & csv file list
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_floder, '/', csv_file))
        print('csv_file_path =', csv_file_path)

        # 2. Get csv dict by csv file path & Process Csv Dict to Get df_all_trx_grouped
        csv_reader = get_csv_dict(csv_file_path)
        df_all_trx_grouped = get_trx_list(csv_reader)[0]
        print('df_all_trx_grouped =', df_all_trx_grouped.iloc[:, 0].size)

        # 3. Get csv dict by csv file path & Process Csv Dict to Get df_td_cap_trx_grouped
        csv_reader = get_csv_dict(csv_file_path)
        df_td_cap_trx_grouped = get_trx_list(csv_reader)[1]
        print('df_td_cap_trx_grouped =', df_td_cap_trx_grouped.iloc[:, 0].size)

        # 4. Get csv dict by csv file path & Process Csv Dict to Get df_zy_cap_trx_grouped
        csv_reader = get_csv_dict(csv_file_path)
        df_zy_cap_trx_grouped = get_trx_list(csv_reader)[2]
        print('df_zy_cap_trx_grouped =', df_zy_cap_trx_grouped.iloc[:, 0].size)

        # 5. Merge All Trx & TD_Device Trx as merge_td
        merge_td = pd.merge(df_all_trx_grouped, df_td_cap_trx_grouped, how='left', on=['plat_date', 'prod_id'])
        merge_td.columns = ['plat_date', 'prod_id', 'trx_cnt', 'td_cap_cnt']

        # 6. Merge merge_td & ZY_Device Trx as merge_td_zy
        merge_td_zy = pd.merge(merge_td, df_zy_cap_trx_grouped, how='left', on=['plat_date', 'prod_id'])
        merge_td_zy.columns = ['plat_date', 'prod_id', 'trx_cnt', 'td_cap_cnt', 'zy_cap_cnt']

        # 7. Add tdCapRate, zyCapRate, batchNo in merge_td_zy as Final df
        merge_td_zy['td_cap_rate'] = merge_td_zy['td_cap_cnt'] / merge_td_zy['trx_cnt']
        merge_td_zy['zy_cap_rate'] = merge_td_zy['zy_cap_cnt'] / merge_td_zy['trx_cnt']
        merge_td_zy['batch_no'] = time.strftime("%Y%m%d%H%M%S")

        rst = rst.append(merge_td_zy, ignore_index=True)
    return rst


def get_trx_list2(parm_csv_reader):
    df_filtered_trx = pd.DataFrame({})
    for line in parm_csv_reader:
        # if 1 < parm_csv_reader.line_num < 100000:
            inst_trace = line['机构请求流水']
            plat_date = line['平台日期'].replace('-', '')
            td_device = line['同盾设备指纹']
            zy_device = line['自研设备指纹']
            prod_id = line['支付产品编号'][0:8]
            if '' != plat_date:
                # Filter Transaction which has Both TD & ZY Device Fingerprint
                if td_device != "" and td_device != '111111' \
                        and zy_device != "" and zy_device != '000000':
                    filtered_trx = pd.DataFrame({'plat_date': [plat_date], 'prod_id': [prod_id],
                                                 'inst_trace': inst_trace,
                                                 'td_device': [td_device], 'zy_device': [zy_device]})
                    df_filtered_trx = df_filtered_trx.append(filtered_trx)
    print('df_filtered_trx \n', df_filtered_trx)
    print('df_filtered_trx =', df_filtered_trx.iloc[:, 0].size)
    return df_filtered_trx


def get_ds_recg_rate(parm_csv_floder, parm_csv_file_list):
    rst = pd.DataFrame({})
    for csv_file in parm_csv_file_list:
        # 1. Get csv files path by csv floder & csv file list
        csv_file_path = os.path.join('%s%s%s' % (parm_csv_floder, '/', csv_file))
        print('csv_file_path =', csv_file_path)

        # 2. Get csv dict by csv file path & Process Csv Dict to Get df_all_trx_grouped
        csv_reader = get_csv_dict(csv_file_path)
        df_filtered_trx = get_trx_list2(csv_reader)



        # 3. Get PRODUCT_ID, START_DATE, END_DATE
        rtn_prod_id = df_filtered_trx.iloc[0]['prod_id']
        df_filtered_trx_sort = df_filtered_trx.sort_values(by=['plat_date'], ascending=[True])
        # print('df_filtered_trx_sort\n', df_filtered_trx_sort)
        start_date = df_filtered_trx_sort.iloc[0]['plat_date']
        end_date = df_filtered_trx_sort.iloc[-1]['plat_date']
        rtn_date_range = start_date + '-' + end_date
        print('rtn_prod_id = ', rtn_prod_id)
        print('rtn_date_range = ', rtn_date_range)

        # 4. Find Duplicated ZY_Device ==>> dup_zy_di
        df_filtered_trx_grouped = df_filtered_trx.groupby(
            [df_filtered_trx['prod_id'], df_filtered_trx['zy_device']]).size().reset_index()
        df_filtered_trx_grouped.columns = ['prod_id', 'zy_device', 'zyCapCnt']
        # print('df_filtered_trx_grouped\n', df_filtered_trx_grouped)
        dup_zy_di = df_filtered_trx_grouped[df_filtered_trx_grouped['zyCapCnt'] > 1]['zy_device'].reset_index()
        dup_zy_di.columns = ['index', 'dup_zy_device']

        # 5. Count Duplicated ZY_Device ==>> dup_zy_di_cnt
        dup_zy_di_cnt = len(dup_zy_di)

        # 6. Find TD_Device By Duplicated ZY_Device ==>> td_di
        td_di = pd.merge(dup_zy_di[['dup_zy_device']],
                         df_filtered_trx[['td_device', 'zy_device']],
                         how='inner',
                         left_on='dup_zy_device',
                         right_on='zy_device')

        # 7. Count No-Duplicated TD_Device ==>> nodup_td_di_cnt
        nodup_td_di_cnt = len(td_di.drop_duplicates(['td_device']))

        # 8. Concatenate Return DataDataFrame ==>> rtn_df
        # print('dup_zy_di_cnt = ', dup_zy_di_cnt)
        # print('nodup_td_di_cnt = ', nodup_td_di_cnt)

        rtn_df = pd.DataFrame({'date_range': [rtn_date_range],
                               'prod_id': [rtn_prod_id],
                               'zy_recg_cnt': [dup_zy_di_cnt],
                               'td_recg_cnt': [nodup_td_di_cnt],
                               'batch_no': time.strftime("%Y%m%d%H%M%S")}, index=None)

        rst = rst.append(rtn_df, ignore_index=True)
    #     print('rst \n', rst)
    #     print('for rst =', rst.iloc[:, 0].size)
    # print('rst =', rst.iloc[:, 0].size)
    return rst


def get_ds_cap_rate_bak(csv_reader):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name,
          sys._getframe().f_lineno, 'csv_reader = ', csv_reader)
    df_all_trx = pd.DataFrame({})
    df_td_cap_trx = pd.DataFrame({})
    df_zy_cap_trx = pd.DataFrame({})
    for line in csv_reader:
        # if 1 < csv_reader.line_num < 3000:
        # order_date = line['商户订单日期']
        plat_date = line['平台日期'].replace('-', '')
        inst_trace = line['机构请求流水']
        prod_id = line['支付产品编号'][0:8]
        if len(prod_id) == 8 and '' != plat_date:
            # 1.Count All Transaction & Group By
            all_trx = pd.DataFrame({'plat_date': [plat_date], 'inst_trace': [inst_trace], 'prod_id': [prod_id]})
            df_all_trx = df_all_trx.append(all_trx)
            df_all_trx_grouped = df_all_trx.groupby(
                [df_all_trx['plat_date'], df_all_trx['prod_id']]).size().reset_index()

            # 2.Count Transaction which TD_Device Capture & Group By
            if line['同盾设备指纹'] != "" and line['同盾设备指纹'] != '111111':
                td_cap_trx = pd.DataFrame(
                    {'plat_date': [plat_date], 'inst_trace': [inst_trace], 'prod_id': [prod_id]})
                df_td_cap_trx = df_td_cap_trx.append(td_cap_trx)
                df_td_cap_trx_grouped = df_td_cap_trx.groupby(
                    [df_td_cap_trx['plat_date'], df_td_cap_trx['prod_id']]).size().reset_index()
            # 3.Count Transaction which ZY_Device Capture & Group By
            if line['自研设备指纹'] != "" and line['自研设备指纹'] != '000000':
                zy_cap_trx = pd.DataFrame(
                    {'plat_date': [plat_date], 'inst_trace': [inst_trace], 'prod_id': [prod_id]})
                df_zy_cap_trx = df_zy_cap_trx.append(zy_cap_trx)
                df_zy_cap_trx_grouped = df_zy_cap_trx.groupby(
                    [df_zy_cap_trx['plat_date'], df_zy_cap_trx['prod_id']]).size().reset_index()

    # 4.Merge All Trx & TD_Device Trx as merge_td
    merge_td = pd.merge(df_all_trx_grouped, df_td_cap_trx_grouped, how='left', on=['plat_date', 'prod_id'])
    merge_td.columns = ['plat_date', 'prod_id', 'trx_cnt', 'td_cap_cnt']

    # 5.Merge merge_td & ZY_Device Trx as merge_td_zy
    merge_td_zy = pd.merge(merge_td, df_zy_cap_trx_grouped, how='left', on=['plat_date', 'prod_id'])
    merge_td_zy.columns = ['plat_date', 'prod_id', 'trx_cnt', 'td_cap_cnt', 'zy_cap_cnt']

    # 6.Add tdCapRate, zyCapRate, batchNo in merge_td_zy as Final df
    merge_td_zy['td_cap_rate'] = merge_td_zy['td_cap_cnt'] / merge_td_zy['trx_cnt']
    merge_td_zy['zy_cap_rate'] = merge_td_zy['zy_cap_cnt'] / merge_td_zy['trx_cnt']
    merge_td_zy['batch_no'] = time.strftime("%Y%m%d%H%M%S")

    return merge_td_zy


def get_ds_recg_rate_bak(csv_reader):
    print(os.path.basename(__file__), sys._getframe().f_code.co_name,
          sys._getframe().f_lineno, 'csv_reader = ', csv_reader)
    df_filtered_trx = pd.DataFrame({})
    for line in csv_reader:
        # if 1 < csv_reader.line_num < 100000:
        inst_trace = line['机构请求流水']
        plat_date = line['平台日期'].replace('-', '')
        td_device = line['同盾设备指纹']
        zy_device = line['自研设备指纹']
        prod_id = line['支付产品编号'][0:8]
        if '' != plat_date:
            # 1.Filter Transaction which has Both TD & ZY Device Fingerprint
            if td_device != "" and td_device != '111111' \
                    and zy_device != "" and zy_device != '000000':
                filtered_trx = pd.DataFrame({'plat_date': [plat_date], 'prod_id': [prod_id],
                                             'inst_trace': inst_trace,
                                             'td_device': [td_device], 'zy_device': [zy_device]})
                df_filtered_trx = df_filtered_trx.append(filtered_trx)
    df_filtered_trx = df_filtered_trx.reset_index()
    # print('df_filtered_trx\n', df_filtered_trx)

    # 2.Get PRODUCT_ID, START_DATE, END_DATE
    rtn_prod_id = df_filtered_trx.iloc[0]['prod_id']
    df_filtered_trx_sort = df_filtered_trx.sort_values(by=['plat_date'], ascending=[True])
    # print('df_filtered_trx_sort\n', df_filtered_trx_sort)
    start_date = df_filtered_trx_sort.iloc[0]['plat_date']
    end_date = df_filtered_trx_sort.iloc[-1]['plat_date']
    rtn_date_range = start_date + '-' + end_date
    print('rtn_prod_id = ', rtn_prod_id)
    print('rtn_date_range = ', rtn_date_range)

    # 3.Find Duplicated ZY_Device ==>> dup_zy_di
    df_filtered_trx_grouped = df_filtered_trx.groupby(
        [df_filtered_trx['prod_id'], df_filtered_trx['zy_device']]).size().reset_index()
    df_filtered_trx_grouped.columns = ['prod_id', 'zy_device', 'zyCapCnt']
    # print('df_filtered_trx_grouped\n', df_filtered_trx_grouped)
    dup_zy_di = df_filtered_trx_grouped[df_filtered_trx_grouped['zyCapCnt'] > 1]['zy_device'].reset_index()
    dup_zy_di.columns = ['index', 'dup_zy_device']

    # 4.Count Duplicated ZY_Device ==>> dup_zy_di_cnt
    dup_zy_di_cnt = len(dup_zy_di)

    # 5.Find TD_Device By Duplicated ZY_Device ==>> td_di
    td_di = pd.merge(dup_zy_di[['dup_zy_device']],
                     df_filtered_trx[['td_device', 'zy_device']],
                     how='inner',
                     left_on='dup_zy_device',
                     right_on='zy_device')

    # 6.Count No-Duplicated TD_Device ==>> nodup_td_di_cnt
    nodup_td_di_cnt = len(td_di.drop_duplicates(['td_device']))

    # 6. Concatenate Return DataDataFrame ==>> rtn_df
    print('dup_zy_di_cnt = ', dup_zy_di_cnt)
    print('nodup_td_di_cnt = ', nodup_td_di_cnt)

    rtn_df = pd.DataFrame({'date_range': [rtn_date_range],
                           'prod_id': [rtn_prod_id],
                           'zy_recg_cnt': [dup_zy_di_cnt],
                           'td_recg_cnt': [nodup_td_di_cnt],
                           'batch_no': time.strftime("%Y%m%d%H%M%S")}, index=None)

    # print('rtn_df\n', rtn_df)

    return rtn_df
