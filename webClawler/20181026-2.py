# coding:utf-8
import os
import time
import datetime
import random

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
k = 1920
for i in range(i,urlCnt):
    dir = "D:/vl_update/" + str(i)
    movie_name = os.listdir(dir)
    for temp in movie_name:
        # print(temp)
        old_name = dir + "/" + temp
        new_name = dir + "/" + "HT_" + str(k) + ".jpg"
        # print("old_name =", old_name)
        print("new_name =", new_name)
        k = k+1
        os.rename(old_name, new_name)