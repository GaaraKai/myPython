
import sys
import os

# 当前文件的路径
pyfile_path = os.getcwd()
# 当前文件的父路径
father_path = os.path.abspath(os.path.dirname(pyfile_path) + os.path.sep + ".")
# 当前文件的前两级目录
grader_father = os.path.abspath(os.path.dirname(pyfile_path) + os.path.sep + "..")
print('pyfile_path = ', pyfile_path)
print('father_path = ', father_path)
print('grader_father = ', grader_father)
print(os.path.basename(__file__))
print(os.path.basename(sys.argv[0]))

sys.path.append(pyfile_path)



import sys
def get_cur_info():
    print(sys._getframe().f_code.co_filename)  #当前文件名，可以通过__file__获得
    print(sys._getframe().f_code.co_name)  #当前函数名
    print(sys._getframe().f_lineno) #当前行号



if __name__ == "__main__":
    get_cur_info()