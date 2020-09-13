# -*- coding: utf-8 -*-
"""
@time: 2020/3/18 2:36 下午
@desc:
"""
import sys
from abc import ABC, abstractmethod

from log4py import Logger

from pydbclib.sql import compilers
from pydbclib.utils import get_suffix


class Driver(ABC):

    @property
    @abstractmethod
    def session(self):
        pass

    @abstractmethod
    def execute(self, sql, params=None, **kw):
        pass

    @abstractmethod
    def execute_many(self, sql, params=None, **kw):
        pass

    def bulk(self, sql, params):
        # return self.connection.execute(sql, params).rowcount
        r = self.execute_many(sql, params)
        self.commit()
        return r.rowcount

    @abstractmethod
    def rollback(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def close(self):
        pass


class ResultProxy(object):

    def __init__(self, context):
        self.context = context

    def __getattr__(self, item):
        """不存在的属性都代理到context中去找"""
        if item == "description":
            return self._get_description()
        else:
            return getattr(self.context, item)

    def _get_description(self):
        # 去除hive表名前缀
        # {'pokes.foo': 238, 'pokes.bar': 'val_238'} => {'foo': 238, 'bar': 'val_238'}
        if hasattr(self.context, "description"):
            description = self.context.description
        else:
            description = self.context._cursor_description()
        return [(get_suffix(r[0]), *r[1:]) for r in description] if description else description

    def get_columns(self):
        """获取查询结果的字段名称"""
        return [i[0].lower() for i in self.description]


@Logger.class_logger()
class CommonDriver(Driver):

    def __init__(self, *args, **kwargs):
        driver_param = kwargs.pop("driver")
        self._cursor = None
        if hasattr(driver_param, "cursor"):
            self.driver_name = driver_param.__class__.__module__
            self.dbapi = sys.modules[self.driver_name]
            self.con = driver_param
        else:
            __import__(driver_param)
            self.driver_name = driver_param
            self.dbapi = sys.modules[driver_param]
            self.con = self.dbapi.connect(*args, **kwargs)
        self.compiler = compilers[self.dbapi.paramstyle]

    @property
    def session(self):
        if not self._cursor:
            self._cursor = self.con.cursor()
        return self._cursor

    def execute(self, sql, params=None, **kw):
        sql, params = self.compiler(sql, params).process_one()
        params = params if params else []
        self.logger.info("{}, {}".format(sql, params))
        self.session.execute(sql, params, **kw)
        return ResultProxy(self._cursor)

    def execute_many(self, sql, params=None, **kw):
        sql, params = self.compiler(sql, params).process()
        params = params if params else []
        self.logger.info("{}, {}".format(sql, params))
        self.session.executemany(sql, params, **kw)
        return ResultProxy(self._cursor)

    def rollback(self):
        self.con.rollback()

    def commit(self):
        self.con.commit()

    def close(self):
        if self.session is not None:
            self.session.close()
        if self.con is not None:
            self.con.close()


@Logger.class_logger()
class SQLAlchemyDriver(Driver):

    def __init__(self, *args, **kwargs):
        self.driver_name = "sqlalchemy"
        driver_param = kwargs.pop("driver")
        self._session = None
        from sqlalchemy import engine, create_engine
        if isinstance(driver_param, engine.base.Engine):
            self.engine = driver_param
        else:
            self.engine = create_engine(*args, **kwargs)

    @property
    def session(self):
        if not self._session:
            from sqlalchemy.orm import sessionmaker
            self._session = sessionmaker(bind=self.engine)()
        return self._session

    def execute(self, sql, params=None, **kw):
        self.logger.info("{}, {}".format(sql, params))
        r = self.session.execute(sql, params, **kw)
        return ResultProxy(r)

    def execute_many(self, sql, params=None, **kw):
        self.logger.info("{}, {}".format(sql, params))
        return ResultProxy(self.session.execute(sql, params, **kw))

    def rollback(self):
        self.session.rollback()

    def commit(self):
        self.session.commit()

    def close(self):
        if self.session is not None:
            self.session.close()
