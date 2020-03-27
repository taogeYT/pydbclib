# -*- coding: utf-8 -*-
"""
@time: 2020/3/26 11:30 上午
@desc:
"""
from pydbclib.exceptions import ParameterError


class Database(object):
    """
    数据库操作适配器
    """

    def __init__(self, driver):
        self.driver = driver

    def get_table(self, name):
        return Table(name, self)

    def execute(self, sql, args=None):
        self.driver.execute(sql, args)
        self.driver.commit()

    def read(self, sql, args=None, to_dict=True):
        self.driver.execute(sql, args)
        records = self.driver.fetchall()
        if to_dict:
            columns = [i[0].lower() for i in self.driver.description()]
            records = [dict(zip(columns, i)) for i in records]
        return records

    def read_one(self, sql, args=None, to_dict=True):
        self.driver.execute(sql, args)
        record = self.driver.fetchone()
        if to_dict:
            if record is None:
                return {}
            else:
                columns = [i[0].lower() for i in self.driver.description()]
                return dict(zip(columns, record))
        else:
            return record

    def write(self, sql, args=None):
        self.driver.execute(sql, args)
        rowcount = self.driver.rowcount()
        return rowcount

    def write_many(self, sql, args=None):
        self.driver.execute_many(sql, args)
        rowcount = self.driver.rowcount()
        return rowcount

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


class Table(object):

    def __init__(self, name, db):
        self.name = name
        self.db = db

    @staticmethod
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

    @staticmethod
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

    def update(self, condition, update):
        condition, p1 = self.format_condition(condition)
        update, p2 = self.format_update(update)
        p1.update(p2)
        return self.db.write(f"update {self.name} set {update}{condition}", p1)

    def delete(self, condition):
        condition, param = self.format_condition(condition)
        return self.db.write(f"delete from {self.name}{condition}", param)

    def find_one(self, condition=None):
        condition, param = self.format_condition(condition)
        return self.db.read_one(f"select * from {self.name}{condition}", param)

    def find(self, condition=None):
        condition, param = self.format_condition(condition)
        return self.db.read(f"select * from {self.name}{condition}", param)

    def _get_insert_sql(self, columns):
        return f"insert into {self.name} ({','.join(columns)})" \
               f" values ({','.join([':%s' % i for i in columns])})"

    def insert_one(self, record):
        if isinstance(record, dict):
            columns = record.keys()
            return self.db.write(self._get_insert_sql(columns), record)
        else:
            raise ParameterError("无效的参数")

    def insert_many(self, records):
        if not isinstance(records, (tuple, list)):
            raise ParameterError("records param must iterable")
        sample = records[0]
        if isinstance(sample, dict):
            columns = sample.keys()
            return self.db.write_many(self._get_insert_sql(columns), records)
        else:
            raise ParameterError("无效的参数")

    # def merge(self):
    #     pass
