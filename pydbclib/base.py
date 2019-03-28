"""
通用数据库连接层封装
"""
from collections import OrderedDict
import sys
import os
from pydbclib.utils import reduce_num
from pydbclib.default import place_holder
from pydbclib.sql import handle
from pydbclib.logger import instance_log
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class Connection(object):

    def __init__(self, *args, **kwargs):
        instance_log(self, kwargs.get('debug'))
        self._connect = None
        self.session = None
        self.columns = None
        self._args = args
        self._kwargs = kwargs

    @property
    def connect(self):
        if self._connect is None:
            self.reset()
        return self._connect

    def reset(self, *args, **kwargs):
        if self._connect:
            self.commit()
            self.close()
        if not args and not kwargs:
            args = self._args
            kwargs = self._kwargs
        kwargs.pop('debug', None)
        self.driver_name = kwargs.get("driver")
        kwargs.pop('driver')
        self.placeholder = kwargs.get("placeholder", place_holder.get(self.driver_name, "?"))
        kwargs.pop('placeholder', None)
        self.create_driver()
        self._connect = self.create_con(*args, **kwargs)
        self.session = self.create_session()

    def dbapi(self):
        __import__(self.driver_name)
        return sys.modules[self.driver_name]

    def create_driver(self):
        self.driver = self.dbapi()
        self.DatabaseError = self.driver.DatabaseError
        try:
            self.Error = self.driver.Error
        except AttributeError:
            self.Error = self.DatabaseError
        self.db_error = self.DatabaseError, self.Error

    def create_con(self, *args, **kwargs):
        try:
            con = self.driver.connect(*args, **kwargs)
        except Exception as reason:
            self.log.critical(
                "db connect failed by driver '%s' args: %s kwargs: %s" %
                (self.driver_name, args, kwargs))
            raise reason
        return con

    def create_session(self):
        return self.connect.cursor()

    def execute(self, sql, args=[], num=10000):
        if self._connect is None:
            self.reset()
        is_many = (args and not isinstance(args, dict) and
                   isinstance(args[0], (tuple, list, dict)))
        need_handle = (":" in sql and args and (":" not in self.placeholder))
        if need_handle:
            """
            解析sql及参数转化成对应适配器的标准
            """
            sql, keys = handle(sql, self.placeholder)
            if isinstance(args, dict):
                args = [args[i] for i in keys]
            elif isinstance(args, (list, tuple)) and isinstance(args[0], dict):
                tmp = []
                for record in args:
                    tmp.append([record[i] for i in keys])
                args = tmp
        if args and ":" in sql:
            """
            满足 cx_Oracle 中dict型参数要和sql中参数个完全匹配的标准
            """
            keys = handle(sql, self.placeholder)[1]
            if isinstance(args, dict):
                args = [args[i] for i in keys]
            elif isinstance(args, (list, tuple)) and isinstance(args[0], dict):
                tmp = []
                for record in args:
                    tmp.append([record[i] for i in keys])
                args = tmp
        if is_many:
            count = self.executemany(sql, args, num)
        else:
            count = self.executeone(sql, args)
        self.columns = None
        return count

    def executeone(self, sql, args):
        try:
            self.session.execute(sql, args)
            if args:
                self.log.debug("%s\nParam:%s" % (sql, args))
            else:
                self.log.debug(sql)
            count = self.session.rowcount
        except (self.DatabaseError, self.Error) as reason:
            self.rollback()
            if args:
                self.log.error(
                    'SQL EXECUTE ERROR(SQL: "%s")\nParam:%s' % (sql, args))
            else:
                self.log.error('SQL EXECUTE ERROR(SQL: "%s")' % sql)
            raise reason
        return count

    def executemany(self, sql, args, num, is_first=True):
        length = len(args)
        count = 0
        try:
            for i in range(0, length, num):
                self.session.executemany(sql, args[i:i + num])
                if is_first:
                    if len(args) > 2:
                        self.log.debug(
                            "%s\nParam:[%s\n       %s"
                            "\n           ... ...]" % (sql, args[0], args[1]))
                    else:
                        self.log.debug("%s\nParam:[%s]" % (
                            sql, '\n       '.join(map(str, args))))
                count += self.session.rowcount
        except (self.DatabaseError, self.Error) as reason:
            self.rollback()
            if num <= 10 or length <= 10:
                self.log.warn(reduce_num(num, length))
                self.log.warn("SQL EXECUTEMANY ERROR EXECUTE EVERYONE")
                for record in args[i:i + num]:
                    self.executeone(sql, record)
            else:
                self.executemany(
                    sql, args[i:i + num],
                    num=reduce_num(num, length),
                    is_first=False)
        return count

    def _query_generator(self, sql, args, chunksize):
        self.execute(sql, args)
        columns = [i[0].lower() for i in self.description()]
        self.columns = columns
        res = self.session.fetchmany(chunksize)
        while res:
            yield res
            res = self.session.fetchmany(chunksize)

    def query_ignore(self, sql, args=[]):
        try:
            if args:
                self.log.info("execute sql(%s) ignore error\nParam:%s" % (sql, args))
            else:
                self.log.info("execute sql(%s) ignore error" % sql)
            self.session.execute(sql, args)
            return self.session.fetchall()
        except self.db_error:
            return None

    def query(self, sql, args=[], size=None):
        """
        :param sql: str
        :param args: list or dict
        :param size: int 查询结果每次返回数量
        :return: a list or generator(when zise is not None) with tuple inside
        """
        if size is None:
            self.execute(sql, args)
            columns = [i[0].lower() for i in self.description()]
            self.columns = columns
            rs = self.session.fetchall()
            # print("rs1:", rs)
            res = [tuple(i) for i in rs]
            return res
        else:
            return self._query_generator(sql, args, size)

    def _query_dict_generator(self, sql, Dict, args, chunksize):
        self.execute(sql, args)
        columns = [i[0].lower() for i in self.description()]
        self.columns = columns
        res = self.session.fetchmany(chunksize)
        while res:
            yield [Dict(zip(columns, i)) for i in res]
            res = self.session.fetchmany(chunksize)

    def query_dict(self, sql, args=[], ordered=False, size=None):
        """
        :param sql: str
        :param args: list or dict
        :param ordered: 返回的字典是否是排序字典
        :param size: int 查询结果每次返回数量
        :return: a list or generator(when zise is not None) with dict inside
        """
        Dict = OrderedDict if ordered else dict
        if size is None:
            self.execute(sql, args)
            columns = [i[0].lower() for i in self.description()]
            self.columns = columns
            return [Dict(zip(columns, i)) for i in self.session.fetchall()]
        else:
            return self._query_dict_generator(sql, Dict, args, size)

    def insert(self, sql, args=[], num=10000):
        return self.execute(sql, args, num)

    def description(self):
        return self.session.description

    def rollback(self):
        self.connect.rollback()

    def commit(self):
        self.connect.commit()

    def close(self):
        """
        关闭数据库连接
        """
        if self._connect is not None:
            self.session.close()
            self.connect.close()

    def __enter__(self):
        return self

    def __exit__(self, exctype, excvalue, traceback):
        try:
            if exctype is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()


# class OracleConnection(Connection):
#     driver_name = "cx_Oracle"


# class OdbcConnection(Connection):
#     driver_name = "pyodbc"


# class ImpalaConnection(Connection):
#     driver_name = "impala.dbapi"


# def db_test():
#     # db = OracleConnection('jwdn/password@local:1521/xe')
#     db = OdbcConnection('DSN=mydb;UID=root;PWD=password')
#     res = db.query("select * from TEST")
#     print(res)
#     db = ImpalaConnection(
#         host='172.16.17.18', port=21050,
#         use_kerberos=True, kerberos_service_name="impala",
#         timeout=3600)
#     # res = db.query_dict('show databases')
#     # print(res)
#     # db.execute('use sjck')
#     # res = db.query('show tables')
#     # print(res)
#     # sql = "select * from dc_dict limit ?"
#     # print(db.query(sql, [1]))


# if __name__ == "__main__":
#     db_test()
