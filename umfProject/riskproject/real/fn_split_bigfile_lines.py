import os
import datetime
import sys


def mkSubFile(lines, head, srcName, sub):
    [des_filename, extname] = os.path.splitext(srcName)
    filename = des_filename + '_' + str(sub) + extname
    print('make file: %s' % filename)
    fout = open(filename, encoding='utf-8',mode='w')
    # fout = open(filename, encoding='gbk', mode='w')
    try:
        fout.writelines([head])
        fout.writelines(lines)
        return sub + 1
    finally:
        fout.close()


def splitByLineCount(filename, count):
    fin = open(filename, encoding='utf-8',mode='r')
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
    big_file_path = "D:/github_program/myPython/docs/source/20180809_test.OUT"
    split_line = 500000
    splitByLineCount(big_file_path, split_line)
    end_time = datetime.datetime.now()
    print('start_time =', start_time)
    print('end_time   =', end_time)
    print('diff_Time =', end_time - start_time)
    print('System Processing Done...')