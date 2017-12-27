import csv


opened_file = open('D:/github_program/myPython/docs/csvfiles/ds_stat/171218095044__P14M000K.csv', 'r')
csv_read_result = csv.DictReader(opened_file)

for line in csv_read_result:
    print('11',1)
for line in csv_read_result:
    print('11',3)

opened_file1 = open('D:/github_program/myPython/docs/csvfiles/ds_stat/171218095044__P14M000K.csv', 'r')
csv_read_result1 = csv.DictReader(opened_file1)
for line in csv_read_result1:
    print('22',2)