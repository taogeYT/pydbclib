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
import sqlparse

from pydbclib.exceptions import SQLFormatError


class Compiler(object):
    def __init__(self, sql, parameters):
        self.sql = sql
        self.parameters = parameters

    def process(self):
        return self.sql, self.parameters


class DefaultCompiler(Compiler):
    place_holder = "?"

    def process_one(self):
        if not self.parameters:
            return self.sql, None
        elif isinstance(self.parameters, (list, tuple)):
            sql, keys = self.parse_sql()
            if len(set(keys)) == len(keys):
                return sql, self.parameters
            else:
                postions = self.to_postions(keys)
                return sql, [self.parameters[p] for p in postions]
        else:
            sql, keys = self.parse_sql()
            return sql, tuple(self.parameters[k] for k in keys)

    def process(self):
        if not self.parameters:
            return self.sql, None
        elif isinstance(self.parameters[0], (list, tuple)):
            sql, keys = self.parse_sql()
            if len(set(keys)) == len(keys):
                return sql, self.parameters
            else:
                postions = self.to_postions(keys)
                return sql, [tuple(parameter[p] for p in postions) for parameter in self.parameters]
        else:
            sql, keys = self.parse_sql()
            return sql, [tuple(parameter[k] for k in keys) for parameter in self.parameters]

    @staticmethod
    def to_postions(keys):
        postions = {}
        i = 0
        for k in keys:
            if k not in postions:
                postions[k] = i
                i += 1
        return [postions[k] for k in keys]

    def parse_sql(self):
        parsed = sqlparse.parse(self.sql)
        stmt = parsed[0]
        tokens = list(stmt.flatten())
        keys = []
        for token in tokens:
            if token.ttype is sqlparse.tokens.Name.Placeholder:
                if ":" in token.value:
                    keys.append(token.value[1:])
                    token.value = self.place_holder
                else:
                    raise SQLFormatError(f"无效的占位符{token.value}, 只支持使用':'开头的占位符")
        return "".join(t.value for t in tokens), keys


class PyFormatCompiler(DefaultCompiler):
    place_holder = "%s"


compilers = {
    "standard": Compiler,
    "default": DefaultCompiler,
    "pyformat": PyFormatCompiler
}

default_place_holders = {
    "cx_Oracle": "standard",
    "pymysql": "pyformat",
    "pyodbc": "default",
    "sqlite3": "default",
    "impala": "default"
}


if __name__ == "__main__":
    print(DefaultCompiler("insert into test(id, name, age) values(:a, ':a', :b)", [{"a": 12, "b": "lyt"}]).process())
    print(DefaultCompiler("insert into test(id, name, age) values(:a, ':a', :b)", None).process())
