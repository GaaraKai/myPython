# -*- coding:utf-8 -*-
import csv

total = 0
avg = 0
csvcontent = csv.reader(open('C:/Users/wangrongkai/Desktop/test1208.csv', encoding='utf-8'))
res = []
t = 0
x = 0
for line in csvcontent:
    print(len(line)) # 一条数据里面有几个数据
    print(line[4])
    if line[0] != "":
        t += 1
    # print(line[4].find(''))
    # print(1111)
    # print(line[0].find('ccc'))
    if line[4] == "":
        x += 1
        print(22)
        # print(line)
        res.append(line)
        # print(res)
print('t= %s, x= %s' % (t,x))  # 3
print('xxx= %s' % (x/(t-1)))  # 3
print('len(res)= %s' % (len(res)))  # 3
for i in res:
    print(i)
    # print(i[0])
    # print(i[1])
    # print(i[2])
    # total = total + int(i[2])
print(total)
# avg = total / len(res)
print('%s 收入金额总数:%d\t平均:%d' % ('lala', total, avg))
