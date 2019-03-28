# -*- coding: utf-8 -*-
"""
sql 语句参数名称提取及参数形式替换
可以考虑sqlparse库解析
import sqlparse
sql2 = "select * from asr where uuid=%s"
parsed = sqlparse.parse(sql)
stmt = parsed[0]
parsed = sqlparse.parse(sql1)
stmt = parsed[0]
for token in stmt.flatten():
    token.ttype is sqlparse.tokens.Name.Placeholder
"""
import re
import random

def _quotation_marks_replace(sql, index):
    """引号部分拆分

    :param sql: sql string
    :type sql: str
    :param index: 引号部分索引
    :type index: list
    :return: 隔离单引号后的sql及分离的引号部分
    :rtype: tuple
    """

    # random.sample(['a', 'b', 'c', 'd'], 2)
    prefix = 'sql_part'
    start = 0
    num = 0
    tmp_sql_part1 = {}
    tmp_sql_part2 = {}
    sql_format = []
    for i in index:
        key = prefix+str(num)
        tmp_sql_part1[key] = sql[start:i[0]]
        sql_format.append("{%s}" % key)
        num += 1
        key = prefix+str(num)
        tmp_sql_part1[key] = '{%s}' % key
        tmp_sql_part2[key] = sql[i[0]:i[1]]
        sql_format.append("{%s}" % key)
        start = i[1]
        num += 1
    else:
        key = prefix+str(num)
        tmp_sql_part1[key] = sql[start:]
        sql_format.append("{%s}" % key)

    formated_sql = "".join(sql_format).format(**tmp_sql_part1)
    return formated_sql, tmp_sql_part2

def sql_factory(func):
    """
    sql string middleware, 隔离引号部分
    """

    def wrapper(sql, repl_symbol=None):
        _match_quotation_marks = re.compile(r"""[^']*('[^']*')[^']*""")
        index = []
        rs = _match_quotation_marks.finditer(sql)
        for i in rs:
            # print(i.group(1))
            index.append(list(i.span(1)))
        if index:
            formated_sql, _kw_quotation_marks = _quotation_marks_replace(sql, index)
            # print(formated_sql)
            sql, keys = func(formated_sql, repl_symbol)
            # print(sql)
            return sql.format(**_kw_quotation_marks), keys
        else:
            return func(sql, repl_symbol)
    return wrapper


class Parse(object):

    def __init__(self, sql, repl_symbol):
        self.sql = sql
        self._repl_symbol = repl_symbol
        self.param_names = []
        self._pattern = re.compile(r":(?P<value>\w+)")
        self._init_sql()

    def _gen_replace_func(self):
        def wrapper1(matched):
            nonlocal i
            i = i + 1
            param_name = "p%s" % i
            self.param_names.append(param_name)
            return ":%s" % param_name
        def wrapper2(matched):
            param_name = matched.group("value")
            self.param_names.append(param_name)
            return self._repl_symbol
        i = 0
        if self._repl_symbol is None:
            return wrapper1
        else:
            return wrapper2

    def _init_sql(self):
        self.sql = self._pattern.sub(self._gen_replace_func(), self.sql)

@sql_factory
def handle(sql, repl_symbol=None):
    """
    :param sql: sql语句
    :param repl_symbol: sql中参数替换符号
    :return: 被转化参数格式的sql 和 从原sql中解析出的参数名列表

    >>> handle("select :123,:321,:123 from dual where a=:a")
    ('select :p1,:p2,:p3 from dual where a=:p4', ['p1', 'p2', 'p3', 'p4'])
    >>> handle("select :123,:321,:123 from dual where a=:a", '?')
    ('select ?,?,? from dual where a=?', ['123', '321', '123', 'a'])
    """
    parser = Parse(sql, repl_symbol)
    sql = parser.sql
    keys = parser.param_names
    return sql, keys


if __name__ == "__main__":
    sql, param_names = handle("select :123,:321,:123 from dual where a=:a")
    print(sql, param_names)
    sql, param_names = handle("select :123,':321',':123' from dual where a=:a", '?')
    print(sql, param_names)
