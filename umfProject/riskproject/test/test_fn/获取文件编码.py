#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/7/11 9:45
# @Author  : Wang Rongkai
# @Site    : 
# @File    : 获取文件编码.py
# @Software: PyCharm Community Edition

import chardet


# 获取文件编码类型
def get_encoding(file):
    # 二进制方式读取，获取字节数据，检测类型
    with open(file, 'rb') as f:
        return chardet.detect(f.read())['encoding']


# file_name = "D:/github_program/myPython/docs/rst/0711/0711_50402_6.csv"
# encoding = get_encoding(file_name)
# print(encoding)


path = "D:/github_program/myPython/docs/rst/0711/0711_50402_6.csv"
#path = "E:/t.zip"
f = open(path,'rb')
data = f.read()
print(chardet.detect(data))