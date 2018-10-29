# coding:utf-8
import os
import time

urlCnt = 4
i = 2
for i in range(2,urlCnt):
    dir = "D:/vl_bestrated1/" + str(i)
    movie_name = os.listdir(dir)
    i = i +1
    for temp in movie_name:
        # print(temp)
        old_name = dir + "/" + temp
        new_name = dir + "/" + "HT_" + str(i) + ".jpg"
        print("old_name =", old_name)
        print("new_name =", new_name)
        # os.rename('./movies/' + temp, 'movies/' + new_name)