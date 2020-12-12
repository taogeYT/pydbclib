# -*- coding: utf-8 -*-
"""
@time: 2020/4/15 3:42 下午
@desc:
"""
import os
import sys


def get_dbapi_module(module_name):
    if module_name in sys.modules and hasattr(sys.modules[module_name], "paramstyle"):
        return module_name
    elif '.' in module_name:
        return get_dbapi_module(os.path.splitext(module_name)[0])
    else:
        raise ValueError("Unknown DBAPI")


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


def get_records(result, batch_size, columns=None):
    records = result.fetchmany(1000)
    while records:
        if columns:
            records = [dict(zip(columns, i)) for i in records]
        for record in records:
            yield record
        records = result.fetchmany(batch_size)


def batch_dataset(dataset, batch_size):
    cache = []
    for data in dataset:
        cache.append(data)
        if len(cache) >= batch_size:
            yield cache
            cache = []
    if cache:
        yield cache


def get_suffix(text):
    left, right = os.path.splitext(text)
    return right[1:] if right else left


def demo_connect():
    from . import connect
    db = connect("sqlite:///:memory:")
    db.execute('create table foo(a integer, b varchar(20))')
    record = {"a": 1, "b": "one"}
    db.execute("INSERT INTO foo(a,b) values(:a,:b)", [record] * 10)
    record = {"a": 2, "b": "two"}
    db.execute("INSERT INTO foo(a,b) values(:a,:b)", [record] * 10)
    return db


if __name__ == '__main__':
    print(to_camel_style("hello_world"))
    print(get_suffix('ab'))
