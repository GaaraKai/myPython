# -*- coding: cp936 -*-  
import os
import time
import sys


def mkSubFile(lines, head, srcName, sub):
    [des_filename, extname] = os.path.splitext(srcName)
    filename = des_filename + '_' + str(sub) + extname
    print('make file: %s' % filename)
    fout = open(filename, 'w')
    try:
        fout.writelines([head])
        fout.writelines(lines)
        return sub + 1
    finally:
        fout.close()


def splitByLineCount(filename, count):
    fin = open(filename, 'rb')
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
    filename = \
        "E:/SVN�ĵ�����/08_��Ʒ���/04.��Ʒ�����ĵ�/42.������ȡ_�豸ָ��ά�����ʿͻ�������ϸ_DTQ00005232/OTQ00005232+20180117150857/sql/NO 2_zy"
    begin = time.time()
    splitByLineCount(filename, 600000)
    end = time.time()
    print('time is %d seconds ' % (end - begin))