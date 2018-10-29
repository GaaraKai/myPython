# coding:utf-8
import os
import time
import datetime
import random
import shutil

def get_unique_num():
    nowTime=datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    randomNum=random.randint(0,1000)
    if randomNum<=10:
        randomNum=str(0)+str(randomNum)
    uniqueNum=str(nowTime)+str(randomNum)
    return uniqueNum

def get_unique():
    nowTime=datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    randomNum=random.randint(0,100000)
    if randomNum<=10:
        randomNum=str(0)+str(randomNum)
    uniqueNum=str(nowTime)+str(randomNum)
    return str(randomNum)


urlCnt = 200
i = 2
target = "D:/HT_update/"
for i in range(i,urlCnt):
    dir = "D:/vl_update/" + str(i)
    movie_name = os.listdir(dir)
    for temp in movie_name:
        # print(temp)
        old_name = dir + "/" + temp
        tar_name = target + temp
        print("old_name =", old_name)
        print("tar_name =", tar_name)
        shutil.move(old_name, target)