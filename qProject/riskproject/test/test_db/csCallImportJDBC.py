#!/usr/bin/python3
# coding:utf8
# -*- coding: utf-8 -*-
import csv
import os
import pymysql
import traceback
import time
import datetime


def sql_insert(insert_sql, insert_value):
    conn = pymysql.connect("localhost", "root", "root", "rcdb", charset="utf8")
    cur = conn.cursor()
    print(insert_sql, insert_value)
    try:
        cur.execute(insert_sql, insert_value)
        conn.commit()
        print("Have Done...")
    except:
        print("Error...")
        traceback.print_exc()
        conn.rollback()
    conn.close()


def get_sql():
    sql = 'insert into cs_call_trx_test (risk_no, inst_id, inst_trace, mer_id,mer_name,\
                                    prod_id,prod_name,order_id,id_no,mobile_no,\
                                    card_no,trx_amount,trx_status,rule_no,cre_date,\
                                    cre_time,order_ip,pay_ip,order_loc,pay_loc,\
                                    mobile_loc,batch_no) values\
                                    (%s,%s,%s,%s,%s, \
                                     %s,%s,%s,%s,%s, \
                                     %s,%s,%s,%s,%s, \
                                     %s,%s,%s,%s,%s, \
                                     %s,%s)'
    return sql


def csv_reader(csvFilePath):
    with open(csvFilePath, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        print(csv_reader.fieldnames)
        for row in csv_reader:
            print(csv_reader.line_num)
            # if csv_reader.line_num < 3:
            insert_value = get_value(row)
            insert_sql = get_sql()
            sql_insert(insert_sql, insert_value)


def get_value(row):
    risk_no = row['风险号']
    inst_id = row['机构号']
    inst_trace = row['机构请求流水']
    mer_id = row['商户号']
    mer_name = row['商户名称'].replace(' ', '')
    prod_id = row['产品号']
    prod_name = row['产品名']
    order_id = row['商户订单号']
    id_no = row['证件号/机构代码']
    mobile_no = row['手机号']
    card_no = row['银行卡号']
    trx_amount = float(row['金额（元）'])
    trx_status = row['交易状态']
    rule_no = row['触发规则'][0:10]
    cre_date = row['创建时间'][0:10]
    cre_time = row['创建时间'][-8:]
    order_ip = row['下单IP']
    pay_ip = row['用户支付IP']
    order_loc = row['下单IP归属地']
    pay_loc = row['用户支付IP归属地']
    mobile_loc = row['预留手机号归属地']
    batch_no = time.strftime("%Y%m%d%H%M%S")
    insert_value = (risk_no, inst_id, inst_trace, mer_id, mer_name,
                    prod_id, prod_name, order_id, id_no, mobile_no,
                    card_no, trx_amount, trx_status, rule_no, cre_date,
                    cre_time, order_ip, pay_ip, order_loc, pay_loc,
                    mobile_loc, batch_no)
    return insert_value


if __name__ == '__main__':
    # start_time = time.strftime("%Y%m%d%H%M%S")
    # start_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    start_time = datetime.datetime.now()
    # print(start_time)
    csvFilePath = r'C:\Users\wangrongkai\Desktop\20171208175442.csv'
    csv_reader(csvFilePath)
    end_time = datetime.datetime.now()
    print('start_time =',start_time)
    print('end_time   =', end_time)
    print('diff = ',end_time-start_time)

