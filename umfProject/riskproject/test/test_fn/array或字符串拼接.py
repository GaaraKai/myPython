import numpy as np

a=np.array([1,2,3])
b=np.array([11,22])
c=np.array([44,55,66])
d= np.concatenate((a,b,c),axis=0)  # 默认情况下，axis=0可以不写

print(d)

# array([ 1,  2,  3, 11, 22, 33, 44, 55, 66]) #对于一维数组拼接，axis的值不影响最后的结果
test = []
print ("未改变的数列 %s" %test)


#增加函数  append()
#用法
test.append('e')
test.append(['e'])
insert_value = ('1','2','3')
test.append(insert_value)


print (test)