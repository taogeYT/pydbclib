# -*- coding: utf-8 -*-
"""
    Python Database Connectivity lib
"""
__author__ = "lyt"
__version__ = '1.2.3'
__all__ = ["connection", "Connection"]

from pydbclib.adapter import ConAdapter
from pydbclib import base
from pydbclib import sqlalchemy


def _connection_factory(*args, **kwargs):
    """
    连接工厂
    """
    driver = kwargs.get('driver')
    if driver is None or driver.lower() == "sqlalchemy":
        return sqlalchemy.Connection(*args, **kwargs)
    else:
        return base.Connection(*args, **kwargs)


def connection(*args, **kw):
    """
    数据库连接配置适配
    For example:
        from pydbclib import connection
        with connection("test.db", driver="sqlite3") as db:
            db.execute('create table test (id varchar(4) primary key, name varchar(10))')
            count = db.insert("insert into test (id, name) values('0001', 'lyt')")
            print("插入的行数:", count)
            data = db.query("select id, name from test")
            print("查询到的结果:", data)
            db.execute('drop table test')

    各种数据库连接方式:
        sqlalchemy mode:
            当没有传入driver参数或者driver参数为'sqlalchemy', 为sqlalchemy模式连接数据，
            数据库配置信息及为sqlalchemy连接串格式（'数据库类型+数据库驱动名称://用户名:口令@机器地址:端口号/数据库名'）
            db = connection("oracle://jwdn:password@localhost:1521/xe")
        general adapter mode:
            当指定driver参数，就要按照指定驱动的配置方式去配置数据库信息
            # 通过pymysql驱动连接mysql数据库
            db = connection(host="localhost", user='root', password='password', database='test', driver="pymysql")
            # 通过pyodbc驱动连接mysql数据库
            db = connection('DSN=mydb;UID=root;PWD=password', driver="pyodbc")
            # 通过impala驱动连接impala
            db = connection(
                host='172.16.17.18', port=21050,
                use_kerberos=True, kerberos_service_name="impala",
                timeout=3600, driver="impala.dbapi")

    常用操作函数usage:
        >>> db.query("select id,name from pydbclib_test where name=:1", ['test'])
        []
        >>> db.insert("insert into pydbclib_test(id,name) values(:id, :name)", {'id': 1, 'name': 'test1'})
        1
        >>> db.insert_by_dict('test', {'id': 2, 'name': 'test2'})
        1
        >>> db.query("select id,name from pydbclib_test where name=:name", {'name': 'test'})
        [(1, test)]
        >>> db.dict_query("select id,name from pydbclib_test where name=:name", {'name': 'test'})
        [{'id':1, 'name': 'test'}]
    """
    kw = kw.copy()
    uri = kw.pop('uri', None)
    dsn = kw.pop('dsn', None)
    rs = uri or dsn
    args = (rs,) if rs else () + args
    return ConAdapter(_connection_factory(*args, **kw))


class Connection(ConAdapter):
    """数据库连接操作自定义扩展类

    :param ConAdapter: 数据库操作适配
    For example:

        from pydbclib import Connection
        class MyUDF(Connection):
            def total_data(self, table):
                return self.query("select count(*) from %s" % table)

        with MyUDF("oracle://lyt:lyt@local:1521/xe") as db:
            count = db.test('test')
            print("test表的总数量为:", count)
    """

    def __init__(self, *args, **kw):
        kw = kw.copy()
        uri = kw.pop('uri', None)
        dsn = kw.pop('dsn', None)
        rs = uri or dsn
        args = (rs,) if rs else () + args
        super().__init__(_connection_factory(*args, **kw))
