# -*- coding: utf-8 -*-
"""
@time: 2020/3/26 11:30 上午
@desc:
"""
from abc import ABC, abstractmethod

from pydbclib.exceptions import ParameterError
from pydbclib.record import Records


class BaseDatabase(ABC):
    def __init__(self, driver):
        self.driver = driver

    def get_table(self, name):
        return Table(name, self)

    @abstractmethod
    def execute(self, sql, args=None):
        pass

    @abstractmethod
    def read(self, sql, args=None, to_dict=True):
        pass

    @abstractmethod
    def read_one(self, sql, args=None, to_dict=True):
        pass

    @abstractmethod
    def write(self, sql, args=None):
        pass

    def commit(self):
        self.driver.commit()

    def rollback(self):
        self.driver.rollback()

    def close(self):
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()


class Database(BaseDatabase):
    """
    数据库操作封装
    """

    def execute(self, sql, args=None):
        """
        原生数据库驱动操作方法
        :param sql: sql语句
        :param args: sql语句参数
        """
        self.driver.execute(sql, args)

    def _readall(self, batch_size, columns=None):
        records = self.driver.fetchmany(batch_size)
        while records:
            if columns:
                # columns = [i[0].lower() for i in self.driver.description()]
                records = [dict(zip(columns, i)) for i in records]
            for record in records:
                yield record
            records = self.driver.fetchmany(batch_size)

    def get_columns(self):
        return [i[0].lower() for i in self.driver.description()]

    def read(self, sql, args=None, to_dict=True, batch_size=5000):
        """
        查询返回所有表记录
        :param sql: sql语句
        :param args: sql语句参数
        :param to_dict: 返回记录是否转换成字典形式（True: [{"a": 1, "b": "one"}]， False: [(1, "one)]），默认为True
        :param batch_size: 每次查询返回的缓存的数量，大数据量可以适当提高大小
        :return: 生成器对象
        """
        self.driver.execute(sql, args)
        columns = self.get_columns()
        return Records(self._readall(batch_size, columns), columns, to_dict)

    def read_one(self, sql, args=None, to_dict=True):
        """
        查询返回一条表记录
        :param sql: sql语句
        :param args: sql语句参数
        :param to_dict: 返回记录是否转换成字典形式（True: [{"a": 1, "b": "one"}]， False: [(1, "one)]），默认为True
        :return: to_dict=True {"a": 1, "b": "one"}, to_dict=False (1, "one")
        """
        self.driver.execute(sql, args)
        record = self.driver.fetchone()
        if to_dict:
            if record is None:
                return None
            else:
                columns = self.get_columns()
                return dict(zip(columns, record))
        else:
            return record

    def write(self, sql, args=None):
        """
        数据库写入操作：
        :param sql: sql语句
        :param args: sql语句参数
        :return: 影响行数

        Example:
            rowcount = db.write(
                "insert into foo(a,b) values(:a,:b)",
                {"a": 1, "b": "one"}
            )

            批量写入
            rowcount = db.write(
                "insert into foo(a,b) values(:a,:b)",
                [
                    {"a": 1, "b": "one"},
                    {"a": 2, "b": "two"}
                ]
            )
        """
        from collections.abc import Iterable
        if args is None or isinstance(args, dict):
            self.driver.execute(sql, args)
            rowcount = self.driver.rowcount()
            return rowcount
        elif isinstance(args, Iterable):
            self.driver.execute_many(sql, args)
            rowcount = self.driver.rowcount()
            return rowcount
        else:
            raise ParameterError("'params'参数类型无效")


def format_condition(condition):
    param = {}
    if isinstance(condition, dict):
        expressions = []
        for i, k in enumerate(condition):
            param[f"c{i}"] = condition[k]
            expressions.append(f"{k}=:c{i}")
        condition = " and ".join(expressions)
    condition = f" where {condition}" if condition else ""
    return condition, param


def format_update(update):
    param = {}
    if isinstance(update, dict):
        expressions = []
        for i, k in enumerate(update):
            param[f"u{i}"] = update[k]
            expressions.append(f"{k}=:u{i}")
        update = ",".join(expressions)
    if not update:
        raise ParameterError("'update' 参数不能为空值")
    return update, param


class Table(object):

    def __init__(self, name, db):
        self.name = name
        self.db = db

    def update(self, condition, update):
        """
        表更新操作
        :param condition: 更新条件，字典类型或者sql条件表达式
        :param update: 要更新的字段，字典类型
        :return: 返回影响行数
        """
        condition, p1 = format_condition(condition)
        update, p2 = format_update(update)
        p1.update(p2)
        return self.db.write(f"update {self.name} set {update}{condition}", p1)

    def delete(self, condition):
        """
        删除表中记录
        :param condition: 删除条件，字典类型或者sql条件表达式
        :return: 返回影响行数
        """
        condition, param = format_condition(condition)
        return self.db.write(f"delete from {self.name}{condition}", param)

    def find_one(self, condition=None):
        """
        按条件查询一条表记录
        :param condition: 查询条件，字典类型或者sql条件表达式
        :return: 字典类型，如 {"a": 1, "b": "one"}
        """
        condition, param = format_condition(condition)
        return self.db.read_one(f"select * from {self.name}{condition}", param)

    def find(self, condition=None):
        """
        按条件查询所有符合条件的表记录
        :param condition: 查询条件，字典类型或者sql条件表达式
        :return: 生成器类型
        """
        condition, param = format_condition(condition)
        return self.db.read(f"select * from {self.name}{condition}", param)

    def _get_insert_sql(self, columns):
        return f"insert into {self.name} ({','.join(columns)})" \
               f" values ({','.join([':%s' % i for i in columns])})"

    def insert(self, records):
        """
        表中插入记录
        :param records: 要插入的记录数据，字典or字典列表
        """
        if isinstance(records, dict):
            return self.insert_one(records)
        else:
            return self.insert_many(records)

    def insert_one(self, record):
        """
        表中插入一条记录
        :param record: 要插入的记录数据，字典类型
        """
        if isinstance(record, dict):
            columns = record.keys()
            return self.db.write(self._get_insert_sql(columns), record)
        else:
            raise ParameterError("无效的参数")

    def insert_many(self, records):
        """
        表中插入多条记录
        :param records: 要插入的记录数据，字典集合
        """
        if not isinstance(records, (tuple, list)):
            raise ParameterError("records param must iterable")
        sample = records[0]
        if isinstance(sample, dict):
            columns = sample.keys()
            return self.db.write(self._get_insert_sql(columns), records)
        else:
            raise ParameterError("无效的参数")

    # def merge(self):
    #     pass
