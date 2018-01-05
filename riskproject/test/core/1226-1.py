# -*- coding: UTF-8 -*-

'''
1、读取指定目录下的所有文件
2、读取指定文件，输出文件内容
3、创建一个文件并保存到指定目录
'''
import os


# 遍历指定目录，显示目录下的所有文件名
def eachFile(filepath):
    pathDir = os.listdir(filepath)
    for allDir in pathDir:
        child = os.path.join('%s%s' % (filepath, allDir))
        print(child)
        # .decode('gbk')是解决中文显示乱码问题


if __name__ == '__main__':
    filePathC = "E:/myself/VBA/csv_files/source/ds/0102/test"
    eachFile(filePathC)

