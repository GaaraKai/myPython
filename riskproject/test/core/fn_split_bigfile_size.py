# coding=utf-8
import os, sys



class FileOperationBase:
    def __init__(self, srcpath, despath, chunksize=1024):
        self.chunksize = chunksize
        self.srcpath = srcpath
        self.despath = despath

    def splitFile(self):
        'split the files into chunks, and save them into despath'
        if not os.path.exists(self.despath):
            os.mkdir(self.despath)
        chunknum = 0
        inputfile = open(self.srcpath, 'rb')  # rb 读二进制文件
        try:
            while 1:
                chunk = inputfile.read(self.chunksize)
                if not chunk:  # 文件块是空的
                    break
                chunknum += 1
                filename = os.path.join(self.despath, ("part--%04d" % chunknum))
                fileobj = open(filename, 'wb')
                fileobj.write(chunk)
        except IOError:
            print("read file error\n")
            raise IOError
        finally:
            inputfile.close()
        return chunknum


filename = \
    "E:/SVN文档数据/08_产品风控/04.产品需求文档/42.数据提取_设备指纹维度优质客户交易明细_DTQ00005232/OTQ00005232+20180117150857/sql/NO 1_td"
# a = filename.decode('utf-8')
test = FileOperationBase\
    (filename, "E:/SVN文档数据/08_产品风控/04.产品需求文档/42.数据提取_设备指纹维度优质客户交易明细_DTQ00005232/OTQ00005232+20180117150857/rst/td", 10*1024*1024)
test.splitFile()