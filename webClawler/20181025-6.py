import urllib.request
import re
import os
import urllib

# urllib.request.urlretrieve('http://pics.dmm.co.jp/mono/movie/adult/118docp101/118docp101ps.jpg',"d://01.jpg")

imglist = ['pics.dmm.co.jp/mono/movie/adult/118docp101/118docp101ps.jpg',
           'pics.dmm.co.jp/mono/movie/adult/118yrh173/118yrh173ps.jpg']

x=0
for imgurl in imglist:
    file_name = "d://" + str(x) + ".jpg"
    print(imgurl)
    print(file_name)
    urllib.request.urlretrieve("http://"+imgurl, file_name)
    # urllib.request.urlretrieve(imgurl, '{}{}.jpg'.format(paths, x))
    # 打开imglist中保存的图片网址，并下载图片保存在本地，format格式化字符串
    x = x + 1