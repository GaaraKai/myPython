import urllib3
import re
import webClawler.get_url as get_url
import os
import sys


targetPath = "D:\\1024\\"


def openUrl(url):
    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/51.0.2704.63 Safari/537.36'}
    try:
        req = urllib3.Request(url=url, headers=headers)
        res = urllib3.urlopen(req)
        data = res.read()
        return data
    except:
        print("Network connection error!")

def getUrl(data):
    buf =data
    list_url = re.findall(r'((http|https):[^\s]*?(jpg|png|gif))', buf)
    #print list_url
    return list_url

def savedata(list_url,filePath):
   i = 0
  # print list_url
   for url, t, b in list_url:
           print("downloading:" + url)
           saveFile(url,filePath, i)
           i += 1

def saveFile(url,filePath,i):
      try:
         f = open(filePath+'//' +str(i)+'.jpg', 'wb')
         buf = openUrl(url)
         f.write(buf)
      except:
          print("error")

def start():
    # k = int(input("How many pages to download: "))
    k = 2
    print(2)
    for i in range (1,k):
        print("i = ", i)
        # url_data = get_url.openUrl("http://www.t66y.com/thread0806.php?fid=16&search=&page=",i)
        # url_data = get_url.openUrl("https://www.jianshu.com",i)
        url_data = get_url.openUrl("https://www.qunar.com/",i)

        # url_data = get_url.openUrl("http://www.youdao.com/w/blue%20picture/#keyfrom=dict2.top",i)
        print("##########")
        print(url_data)
        print("##########")
        sys.exit(0)
        if url_data ==None:
            print("Start running again.")
            start()
        else:
            for url in url_data:
                url_a = "http://www.t66y.com/" + url
                filePath = targetPath + url_a[-12:-5]
                if not os.path.isdir(filePath):
                    os.makedirs(filePath)
                    data = openUrl(url_a)
                    list_url = getUrl(data)
                    savedata(list_url, filePath)
                else:
                    print("already downloaded" + url_a)
                    print("filePath  " + filePath)

start()