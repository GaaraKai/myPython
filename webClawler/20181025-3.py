import urllib3
import re
import urllib.request
import sys


def openUrl(url_b, i):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/51.0.2704.63 Safari/537.36'}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.36'
    }
    i = str(i)
    # url = url_b + i
    url = url_b
    print("url = ", url)
    print("headers = ", headers)

    # req = urllib3.Request(url=url, headers=headers)
    # res = urllib3.urlopen(req)
    req = urllib.request.Request(url=url, headers=headers)
    res = urllib.request.urlopen(req)
    # print(req.get_method())
    # print(res)
    data = res.read()
    print(data)

    results = re.findall("(?isu)(http\://[a-zA-Z0-9\.\?/&\=\:]+)",data )
    print(results)
    sys.exit(0)

    list_url = re.findall(r'htm_data.{,80}\.html', data)
    list_url = list(set(list_url))
    print("/////////////")
    print(list_url)
    print("/////////////")


def get_url(data):
    print(222)
    buf = data




    list_url = re.findall(r'htm_data.{,80}\.html', buf)
    print("/////////////")
    print(list_url)
    print("/////////////")
    sys.exit(0)
    list_url = list(set(list_url))
    return list_url


openUrl("https://www.qunar.com/",1)