import urllib.request
import re
import os
import urllib
import sys


# 根据给定的网址来获取网页详细信息，得到的html就是网页的源代码

def getHtml(url,i):
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
    req = urllib.request.Request(url=url, headers=headers)
    page = urllib.request.urlopen(req)
    html = page.read()

    # page = urllib.request.urlopen(url)
    # html = page.read()

    # print(html)
    return html.decode('UTF-8')


def getImg(html,i):
    global x
    reg = r'src="(.+?\.jpg)" width='
    imgre = re.compile(reg)
    # print(imgre)
    imglist = imgre.findall(html)  # 表示在整个网页中过滤出所有图片的地址，放在imglist中

    # sys.exit(0)

    path = 'D:\\HT_NEWONE'
    # print(path)

    if not os.path.isdir(path):
        os.makedirs(path)
    paths = path + '\\'  # 保存在test路径下

    print("########################")
    # print(imglist)
    print("web page = ", i)
    print("PROCESSING...")
    print("########################")
    # sys.exit(0)
    for imgurl in imglist:
        # print(imgurl)
        url_name = "http:"+imgurl
        file_name = paths + "HT_" + str(x) + ".jpg"
        # print(url_name)
        # print(file_name)
        urllib.request.urlretrieve(url_name, file_name)
        x = x + 1
    return imglist


# html = getHtml("http://tieba.baidu.com/p/2460150866")
# 获取该网址网页详细信息，得到的html就是网页的源代码
urlCnt = 26
i = 2
global x
x = 1
url_list = ["http://www.o23g.com/cn/vl_newrelease.php?&mode=&page=",
            # "http://www.o23g.com/cn/vl_newentries.php?&mode=&page=",
            # "http://www.o23g.com/cn/vl_update.php?&mode=&page=",
            # "http://www.o23g.com/cn/vl_mostwanted.php?&mode=&page=",
            # "http://www.o23g.com/cn/vl_bestrated.php?&mode=2&page="
            ]
for single_url in url_list:
    print("single_url = ", single_url)
    for i in range(2,urlCnt):
        url = single_url + str(i)
        # url = "http://www.o23g.com/cn/vl_newentries.php?&mode=&page=" + str(i)
        # url = "http://www.o23g.com/cn/vl_update.php?&mode=&page=" + str(i)
        # url = "http://www.o23g.com/cn/vl_mostwanted.php?&mode=&page=" + str(i)
        # url = "http://www.o23g.com/cn/vl_bestrated.php?&mode=2&page=" + str(i)
        # print("i = ", i)
        html = getHtml(url,i)
        getImg(html,i)
        print("DONE...")

# sys.exit(0)
# for imgurl in imglist:
#     html = getHtml("http://www.o23g.com/cn/")
#     http://www.o23g.com/cn/vl_newrelease.php?&mode=&page=3
#     html = getHtml("http://www.o23g.com/cn/")
#     print(getImg(html))