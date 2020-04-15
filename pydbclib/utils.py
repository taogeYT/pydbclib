# -*- coding: utf-8 -*-
"""
@time: 2020/4/15 3:42 下午
@desc:
"""
import os


def to_camel_style(text):
    res = ''
    j = 0
    for i in text.lower().split('_'):
        if j == 0:
            res = i
        else:
            res = res + i[0].upper() + i[1:]
        j += 1
    return res


def get_suffix(text):
    _, r = os.path.splitext(text)
    return r[1:]


if __name__ == '__main__':
    print(to_camel_style("hello_world"))
