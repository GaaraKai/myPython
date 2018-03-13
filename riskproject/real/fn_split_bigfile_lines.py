import os
import datetime
import sys


def mkSubFile(lines, head, srcName, sub):
    [des_filename, extname] = os.path.splitext(srcName)
    filename = des_filename + '_' + str(sub) + extname
    print('make file: %s' % filename)
    fout = open(filename, encoding='gbk',mode='w')
    # fout = open(filename, encoding='gbk', mode='w')
    try:
        fout.writelines([head])
        fout.writelines(lines)
        return sub + 1
    finally:
        fout.close()


def splitByLineCount(filename, count):
    fin = open(filename, encoding='gbk',mode='r')
    # fin = open(filename, encoding='gbk', mode='r')
    try:
        head = fin.readline()
        buf = []
        sub = 1
        for line in fin:
            buf.append(line)
            if len(buf) == count:
                sub = mkSubFile(buf, head, filename, sub)
                buf = []
        if len(buf) != 0:
            sub = mkSubFile(buf, head, filename, sub)
    finally:
        fin.close()


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    big_file_path = "E:/SVN文档数据/08_产品风控/04.产品需求文档/" \
                    "39.反洗钱_非银行支付机构反洗钱现场检查数据接口/反洗钱现场检查下载结果/data_proc/7-0308" \
                    "/(十)存量特约商户风险等级划分记录(网络支付、预付卡、银行卡收单)20180308110323.csv"
    split_line = 10000
    splitByLineCount(big_file_path, split_line)
    end_time = datetime.datetime.now()
    print('start_time =', start_time)
    print('end_time   =', end_time)
    print('diff_Time =', end_time - start_time)
    print('System Processing Done...')