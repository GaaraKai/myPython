import urllib.request

url = 'https://www.jianshu.com'
# 增加header
headers = {
'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.36'
}
request = urllib.request.Request(url,headers=headers)
response = urllib.request.urlopen(request)
#在urllib里面 判断是get请求还是post请求，就是判断是否提交了data参数
print(request.get_method())