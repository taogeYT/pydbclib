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
    left, right = os.path.splitext(text)
    return right[1:] if right else left


def demo_connect():
    from . import connect
    db = connect("sqlite:///:memory:")
    db.execute('create table foo(a integer, b varchar(20))')
    record = {"a": 1, "b": "one"}
    db.write("INSERT INTO foo(a,b) values(:a,:b)", [record] * 10)
    return db


def hive_connect(host="localhost", port=10000, db="default"):
    from . import connect
    return connect(f'hive://{host}:{port}/{db}')


if __name__ == '__main__':
    print(to_camel_style("hello_world"))
    print(get_suffix('ab'))