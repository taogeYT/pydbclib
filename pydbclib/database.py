# -*- coding: utf-8 -*-
"""
@time: 2020/3/26 11:30 上午
@desc:
"""
from collections.abc import Iterator

from pydbclib.exceptions import ParameterError
from pydbclib.record import Records
from pydbclib.utils import batch_dataset, get_records


class Database(object):
    """
    数据库操作封装
    方法：
        get_table
        execute
        bulk
        read
        read_one
    """

    def __init__(self, driver):
        self.driver = driver

    def get_table(self, name):
        return Table(name, self)

    """
    数据库操作封装
    """

    def execute(self, sql, args=None, autocommit=False):
        """
        执行sql语句：
        :param sql: sql语句
        :param args: sql语句参数
        :param autocommit: 执行完sql是否自动提交
        :return: ResultProxy

        Example:
            db.execute(
                "insert into foo(a,b) values(:a,:b)",
                {"a": 1, "b": "one"}
            )

            对条写入
            db.execute(
                "insert into foo(a,b) values(:a,:b)",
                [
                    {"a": 1, "b": "one"},
                    {"a": 2, "b": "two"}
                ]
            )
        """
        if args is None or isinstance(args, dict):
            res = self.driver.execute(sql, args)
        elif isinstance(args, (list, tuple)):
            res = self.driver.execute_many(sql, args)
        else:
            raise ParameterError("'params'参数类型无效")
        if autocommit:
            self.commit()
        return res

    def bulk(self, sql, args, batch_size=100000):
        """批量插入"""
        if isinstance(args, (list, tuple, Iterator)):
            rowcount = 0
            for batch in batch_dataset(args, batch_size):
                rowcount += self.driver.bulk(sql, batch)
            return rowcount
        else:
            raise ParameterError("'params'参数类型无效")

    def read(self, sql, args=None, as_dict=True, batch_size=10000):
        """
        查询返回所有表记录
        :param sql: sql语句
        :param args: sql语句参数
        :param as_dict: 返回记录是否转换成字典形式（True: [{"a": 1, "b": "one"}]， False: [(1, "one)]），默认为True
        :param batch_size: 每次查询返回的缓存的数量，大数据量可以适当提高大小
        :return: 生成器对象
        """
        r = self.driver.execute(sql, args)
        if as_dict:
            # columns = [i[0].lower() for i in r.description]
            columns = r.get_columns()
            records = get_records(r, batch_size, columns)
        else:
            records = get_records(r, batch_size)
        return Records(records, as_dict)

    def read_one(self, sql, args=None, as_dict=True):
        """
        查询返回一条表记录
        :param sql: sql语句
        :param args: sql语句参数
        :param as_dict: 返回记录是否转换成字典形式（True: [{"a": 1, "b": "one"}]， False: [(1, "one)]），默认为True
        :return: to_dict=True {"a": 1, "b": "one"}, to_dict=False (1, "one")
        """
        r = self.driver.execute(sql, args)
        record = r.fetchone()
        # Unbuffered Cursor needed
        r.fetchall()
        if as_dict:
            if record is None:
                return None
            else:
                # columns = [i[0].lower() for i in r.description]
                columns = r.get_columns()
                return dict(zip(columns, record))
        else:
            return record

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
    """
    数据库表操作封装
    方法：
        get_columns
        insert
        bulk
        update
        delete
        find_one
        find
    """

    def __init__(self, name, db):
        self.name = name
        self.db = db

    def get_columns(self):
        """获取表字段名称"""
        r = self.db.execute(f"select * from {self.name} where 1=0")
        r.fetchall()
        # return [i[0].lower() for i in r.description]
        return r.get_columns()

    def insert(self, records):
        """
        表中插入记录
        :param records: 要插入的记录数据，字典or字典列表
        """
        if isinstance(records, dict):
            return self._insert_one(records)
        else:
            return self._insert_many(records)

    def bulk(self, records, batch_size=100000):
        if isinstance(records, (list, tuple, Iterator)):
            rowcount = 0
            for batch in batch_dataset(records, batch_size):
                rowcount += self._insert_many(batch)
                self.db.commit()
            return rowcount
        else:
            raise ParameterError("'params'参数类型无效")

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
        return self.db.execute(f"update {self.name} set {update}{condition}", p1).rowcount

    def delete(self, condition):
        """
        删除表中记录
        :param condition: 删除条件，字典类型或者sql条件表达式
        :return: 返回影响行数
        """
        condition, param = format_condition(condition)
        return self.db.execute(f"delete from {self.name}{condition}", param).rowcount

    def find_one(self, condition=None, fields=None):
        """
        按条件查询一条表记录
        :param condition: 查询条件，字典类型或者sql条件表达式
        :param fields: 指定返回的字段
        :return: 字典类型，如 {"a": 1, "b": "one"}
        """
        if fields is None:
            fields = "*"
        else:
            fields = ','.join(fields)
        condition, param = format_condition(condition)
        return self.db.read_one(f"select {fields} from {self.name}{condition}", param)

    def find(self, condition=None, fields=None):
        """
        按条件查询所有符合条件的表记录
        :param condition: 查询条件，字典类型或者sql条件表达式
        :param fields: 指定返回的字段
        :return: 生成器类型
        """
        if fields is None:
            fields = "*"
        else:
            fields = ','.join(fields)
        condition, param = format_condition(condition)
        return self.db.read(f"select {fields} from {self.name}{condition}", param)

    def _get_insert_sql(self, columns):
        return f"insert into {self.name} ({','.join(columns)})" \
               f" values ({','.join([':%s' % i for i in columns])})"

    def _insert_one(self, record):
        """
        表中插入一条记录
        :param record: 要插入的记录数据，字典类型
        """
        if isinstance(record, dict):
            columns = record.keys()
            return self.db.execute(self._get_insert_sql(columns), record).rowcount
        else:
            raise ParameterError("无效的参数")

    def _insert_many(self, records):
        """
        表中插入多条记录
        :param records: 要插入的记录数据，字典集合
        """
        if not isinstance(records, (tuple, list)):
            raise ParameterError("records param must list or tuple")
        sample = records[0]
        if isinstance(sample, dict):
            columns = sample.keys()
            return self.db.execute(self._get_insert_sql(columns), records).rowcount
        else:
            raise ParameterError("无效的参数")
