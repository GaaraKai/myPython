import urllib.request

url = 'http://httpbin.org/ip'
proxy = {'http':'39.134.108.89:8080','https':'39.134.108.89:8080'}
proxies = urllib.request.ProxyHandler(proxy) # 创建代理处理器
opener = urllib.request.build_opener(proxies,urllib.request.HTTPHandler) # 创建特定的opener对象
urllib.request.install_opener(opener) # 安装全局的opener 把urlopen也变成特定的opener
data = urllib.request.urlopen(url)
print(data.read().decode())