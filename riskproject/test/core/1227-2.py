import csv


# opened_file = open('D:/github_program/myPython/docs/csvfiles/ds_stat/171218095044__P14M000K.csv', 'r')
# csv_read_result = csv.DictReader(opened_file)
#
# for line in csv_read_result:
#     print('11',1)
# for line in csv_read_result:
#     print('11',3)
#
# opened_file1 = open('D:/github_program/myPython/docs/csvfiles/ds_stat/171218095044__P14M000K.csv', 'r')
# csv_read_result1 = csv.DictReader(opened_file1)
# for line in csv_read_result1:
#     print('22',2)


def get_csv_dict(parm_path):
    return csv.DictReader(open(parm_path, 'r'))
csv_reader = get_csv_dict('D:/github_program/myPython/docs/csvfiles/ds_stat/180102101644__P1350000.csv')

for line in csv_reader:
    print(line)
