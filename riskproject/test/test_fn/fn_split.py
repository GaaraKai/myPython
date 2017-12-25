#!/usr/bin/python3

import re


def go_split(s, symbol):
    # 拼接正则表达式
    symbol = "[" + symbol + "]+"
    # 一次性分割字符串
    result = re.split(symbol, s)
    # 去除空字符
    return [x for x in result if x]


if __name__ == "__main__":
    # 定义初始字符串
    s = '12;;7.osjd;.jshdjdknx+'
    # 定义分隔符
    symbol = ';./+'

    result = go_split(s, symbol)
    print(result)