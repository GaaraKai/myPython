import pandas as pd
import datetime
import numpy as np

start_time = datetime.datetime.now()

flights = pd.read_csv('D:/github_program/myPython/riskproject/test/learn_test/heat_map.csv')

# 1、 列名
print("**********************-1-*************************")
print(type(flights.columns))
print(flights.columns)
print("***********************************************\n")

# 2、 大小（行数，列数）
print("**********************-2-*************************")
print(type(flights.shape))
print (flights.shape)
print("***********************************************\n")

# 3、 第N行数据
print("**********************-3-*************************")
print(flights.loc[0])
print("***********************************************\n")

# 4. 查看列类型
# object - For string values
# int - For integer values
# float - For float values
# datetime - For time values
# bool - For Boolean values
print("**********************-4-*************************")
print(flights.dtypes)
print("***********************************************\n")

# 5.1 Returns a DataFrame containing the rows at indexes 3, 4, 5, and 6.
print("**********************-5.1-*************************")
print(flights.loc[3:6])
# 5.2 Returns a DataFrame containing the rows at indexes 2, 5, and 10.
print("**********************-5.2-*************************")
two_five_ten = [2,5,10]
print(flights.loc[two_five_ten])
print("***********************************************\n")

# 6. Series object representing the "procID" column.
print("**********************-6.1-*************************")
procID_col = flights["procID"]
print(procID_col.tail())
print("**********************-6.2-*************************")
columns = ["payDate", "tdCapRate"]
zinc_copper = flights[columns]
print(zinc_copper.tail())
print("***********************************************\n")

# 7. 将列名中以“e”结尾的列名找出来，组成一个新的DF
# 7.1 将列名转换为一个List
print("**********************-7.1-*************************")
col_names = flights.columns.tolist()
print(col_names)
# 7.2 将列名中以“e”结尾的列名放入新建list（gram_columns）中
print("**********************-7.2-*************************")
gram_columns = []
for c in col_names:
    if c.endswith("e"):
        gram_columns.append(c)
# 7.3 新建DF（gram_df），其列名为list（gram_columns）中每个元素
print("**********************-7.3-*************************")
gram_df = flights[gram_columns]
print(gram_df.tail(3))
print("***********************************************\n")

# 8. Multiplies each value in the column by 2 and returns a Series object.
print("**********************-8.1-*************************")
print(flights["tdCapRate"].tail(3))
print("**********************-8.2-*************************")
mult_2 = flights["tdCapRate"] * 100
print(mult_2.tail(3))
# 8.3 新增列，shape增加，from 3 to 4
print("**********************-8.3-*************************")
print (flights.shape)
flights["new_col"] = mult_2
print (flights.shape)
print("***********************************************\n")

# 9. #By default, pandas will sort the data by the column we specify in ascending order and return a new DataFrame
# 9.1 Sorts the DataFrame in-place, rather than=true returning a new DataFrame.
# 如果增加inplace=True，则改变原始DF
print("**********************-9.1-*************************")
flights.sort_values("tdCapRate", inplace=True)
print (flights.tail(5))
print (flights["tdCapRate"].tail(5))
# 9.2 Sorts by descending order, rather than ascending.
# ascending=False，代表降序，默认是升序
print("**********************-9.2-*************************")
flights.sort_values("tdCapRate", inplace=True, ascending=False)
print(flights["tdCapRate"].tail(5))
print("***********************************************\n")

# 10.空数据处理
#The Pandas library uses NaN, which stands for "not a number", to indicate a missing value.
#we can use the pandas.isnull() function which takes a pandas series and returns a series of True and False values
print("**********************-10-*************************")
df = pd.read_csv('D:/github_program/myPython/riskproject/test/learn_test/heat_map_null.csv')
print(len(df))
# 10.1 判断某一列中的空数据（NaN），返回一个bool列
print("**********************-10.1-*************************")
age = df["tdCapRate"]
age_is_null = pd.isnull(age)
print (age_is_null)
# 10.2 根据false,找到空数据
print("**********************-10.2-*************************")
age_null_true = age[age_is_null]
print (age_null_true)
# 10.3 统计空数据个数
print("**********************-10.3-*************************")
age_null_count = len(age_null_true)
print(age_null_count)
print("***********************************************\n")
# 10.4 只选取非空数据，放入good_ages
print("**********************-10.4-*************************")
good_ages = df["tdCapRate"][age_is_null == False]
print(good_ages)
print(len(good_ages))
print("***********************************************\n")

# 11 透视图
print("**********************-11-*************************")
#index tells the method which column to group by
#values is the column that we want todf apply the calculation to
#aggfunc specifies the calculation we want to perform
print("**********************-11.1-*************************")
df1 = df.pivot_table(index="procID", values="tdCapRate", aggfunc=np.mean)
print(df1)
print("**********************-11.2-*************************")
df2 = df.pivot_table(index="procID", values=["payDate","tdCapRate"], aggfunc=np.mean)
print(df2)
print("***********************************************\n")

















end_time = datetime.datetime.now()
print('start_time =', start_time)
print('end_time   =', end_time)
print('diff_Time =', end_time - start_time)
print('System Processing Done...')