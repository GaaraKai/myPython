#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
功能：通过打开一个文件对话窗口来选择文件，获得文件路径（包含文件名和后缀）
时间：2017年3月10日 15:40:06
"""

import os
import tkinter.filedialog as tf

default_dir = r"D:\PythonTest\connMySQL\csv\csv_test_1218"  # 设置默认打开目录
fname = tf.askopenfilename(title=u"选择文件CSV文件",filetypes= [("csv files", "*.csv"),("all files", "*")],
                           initialdir=(os.path.expanduser(default_dir)),
                           initialfile='')


# defaultextension = s 默认文件的扩展名
# filetypes = [(label1, pattern1), (label2, pattern2), …] 设置文件类型下拉菜单里的的选项
# initialdir = D 对话框中默认的路径
# initialfile = F 对话框中初始化显示的文件名
# parent = W 父对话框(由哪个窗口弹出就在哪个上端)
# title = T 弹出对话框的标题
# 如果选中文件的话,确认后会显示文件的完整路径,否则单击取消的话会返回空字符串


print(fname)  # 返回文件全路径
print(fname.split('/')[-1])
print(fname.split('/')[-2])
# print(tf.askdirectory())
# print(tf.askdirectory())  # 返回目录路径