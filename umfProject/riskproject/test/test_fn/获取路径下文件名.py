#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/7 17:19
# @Author  : Wang Rongkai
# @Site    : 
# @File    : 获取路径下文件名.py
# @Software: PyCharm Community Edition

import os

def traverse(f):
    fs = os.listdir(f)
    i = 1
    for f1 in fs:
        # print('f1 =  %s' % f1)
        tmp_path = os.path.join(f, f1)
        # print('tmp_path =  %s' % tmp_path)

        if not os.path.isdir(tmp_path):
            # print('文件i: %s' % i)
            print('文件: %s' % tmp_path)
            i = i + 1
        else:
            # print('文件夹i：%s' % i)
            print('文件夹：%s' % tmp_path)
            i = i + 1
            traverse(tmp_path)


path = "E:\\SVN文档数据\\08_产品风控\\"
traverse(path)
