"""
基于sqlalchemy数据库连接层封装
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import DatabaseError, DBAPIError
from collections import OrderedDict
from sqlalchemy import engine
import re
import os
# import sys
from pydbclib.utils import reduce_num
from pydbclib.sql import handle
from pydbclib.logger import instance_log
# from collections import property
# from pydbclib.error import ConnectError, ExecuteError
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class Connection(object):

    def __init__(self, dsn, debug=False, echo=False):
        """
        :param dsn: sqlalchemy.create_engine url parameter, for example: "oracle+cx_oracle://jwdn:password@192.168.152.1:1521/xe"
        :param debug: print debug info
        :param echo: sqlalchemy.create_engine echo parameter
        """
        instance_log(self, debug)
        self._connect = None
        self.session = None
        self.columns = None
        self.disable_log = False
        self._dsn = dsn
        self._echo = echo
        self.db_error = DatabaseError, DBAPIError

    # def print(self, *args):
    #     self.log.

    @property
    def connect(self):
        if self._connect is None:
            self.reset()
        return self._connect

    def reset(self, dsn=None):
        if self._connect and dsn:
            self.commit()
            self.close()
        if dsn is None:
            dsn = self._dsn
        if hasattr(dsn, 'session'):
            self._connect = dsn.engine
            self.session = dsn.session
        else:
            try:
                self._connect = dsn if isinstance(
                    dsn, engine.base.Engine) else create_engine(dsn, echo=self._echo)  # poolclass=NullPool
            except Exception as reason:
                self.log.critical(
                    "db connect failed args: %s, "
                    "Usage example: 'oracle+cx_oracle://user:pwd@local:1521/xe'" %
                    dict(dsn=dsn, echo=self._echo))
                raise reason
            self.session = self.create_session()

    def create_session(self):
        DB_Session = sessionmaker(bind=self._connect)
        return DB_Session()

    def execute(self, sql, args=[], num=10000):
        if self._connect is None:
            self.reset()
        is_list = (':' in sql and args and isinstance(args, (list, tuple)) and
                   not isinstance(args[0], dict))
        is_many = (args and not isinstance(args, dict) and
                   isinstance(args[0], (list, tuple, dict)))
        if is_list:
            sql, keys = handle(sql)
            if isinstance(args[0], (list, tuple)):
                args = [dict(zip(keys, i)) for i in args]
            else:
                args = dict(zip(keys, args))
        if is_many:
            rs = self.executemany(sql, args, num)
        else:
            rs = self.executeone(sql, args)
        self.columns = None
        return rs

    def executeone(self, sql, args):
        try:
            rs = self.session.execute(sql, args)
            if args:
                self.log.debug("%s\nParam:%s" % (sql, args))
            else:
                self.log.debug(sql)
        except (DatabaseError, DBAPIError) as reason:
            self.rollback()
            if args:
                self.log.error(
                    'SQL EXECUTE ERROR(SQL: "%s")\nParam:%s' % (sql, args))
            else:
                self.log.error('SQL EXECUTE ERROR(SQL: "%s")' % sql)
            raise reason
        return rs

    def executemany(self, sql, args, num, is_first=True):
        length = len(args)
        count = 0
        try:
            for i in range(0, length, num):
                rs = self.session.execute(sql, args[i:i + num])
                if is_first:
                    if len(args) > 2:
                        self.log.debug(
                            "%s\nParam:[%s\n       %s"
                            "\n           ... ...]" % (sql, args[0], args[-1]))
                    else:
                        self.log.debug("%s\nParam:[%s]" % (
                            sql, '\n       '.join(map(str, args))))
                count += rs.rowcount
        except (DatabaseError, DBAPIError) as reason:
            self.rollback()
            if num <= 10 or length <= 10:
                self.log.warn("SQL EXECUTEMANY ERROR EXECUTE EVERYONE")
                for record in args[i:i + num]:
                    rs = self.executeone(sql, record)
            else:
                self.executemany(
                    sql, args[i:i + num],
                    num=reduce_num(num, length),
                    is_first=False)
        rs.rowcount = count
        return rs

    def _query_generator(self, sql, args, chunksize):
        rs = self.execute(sql, args)
        columns = [i[0].lower() for i in rs._cursor_description()]
        self.columns = columns
        res = rs.fetchmany(chunksize)
        while res:
            yield res
            res = rs.fetchmany(chunksize)

    def query_ignore(self, sql, args=[]):
        if self._connect is None:
            self.reset()
        try:
            if args:
                self.log.info("execute sql(%s) ignore error\nParam:%s" % (sql, args))
            else:
                self.log.info("execute sql(%s) ignore error" % sql)
            rs = self.session.execute(sql, args)
            return rs.fetchall()
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
            rs = self.execute(sql, args)
            columns = [i[0].lower() for i in rs._cursor_description()]
            self.columns = columns
            rs = rs.fetchall()
            # print("rs:", rs)
            res = [tuple(i) for i in rs]
            return res
        else:
            return self._query_generator(sql, args, size)

    def _query_dict_generator(self, sql, Dict, args, chunksize):
        rs = self.execute(sql, args)
        columns = [i[0].lower() for i in rs._cursor_description()]
        self.columns = columns
        res = rs.fetchmany(chunksize)
        while res:
            yield [Dict(zip(columns, i)) for i in res]
            res = rs.fetchmany(chunksize)

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
            rs = self.execute(sql, args)
            columns = [i[0].lower() for i in rs._cursor_description()]
            self.columns = columns
            return [Dict(zip(columns, i)) for i in rs.fetchall()]
        else:
            return self._query_dict_generator(sql, Dict, args, size)

    def _modify_field_size(self, reason):
        """
        修改oracle varchar2 类型字段大小
        """
        match_reason = re.compile(
            r'"\w+"\."(\w+)"\."(\w+)" 的值太大 \(实际值: (\d+),')
        rs = match_reason.search(str(reason))
        table_name, column_name, value = (
            rs.group(1), rs.group(2), rs.group(3))
        delta = 2 if len(
            str(value)) == 1 else 2 * 10**(len(str(value)) - 2)
        sql_query = ("select data_type,char_used from user_tab_columns"
                     " where table_name='%s' and column_name='%s'" % (
                         table_name, column_name))
        data_type, char_used = self.query(sql_query)[0]
        if char_used == 'C':
            char_type = 'char'
        elif char_used == 'B':
            char_type = 'byte'
        else:
            char_type = ''
        sql_modify = ("alter table {table_name} modify({column_name}"
                      " {data_type}({value} {char_type}))".format(
                          table_name=table_name,
                          column_name=column_name,
                          data_type=data_type,
                          char_type=char_type,
                          value=int(value) + delta))
        self.execute(sql_modify)

    # def insert(self, sql, args=[], num=10000):
    #     """
    #     批量更新插入，默认超过10000条数据自动commit
    #     insertone:
    #         self.insert(
    #             "insert into test(id) values(:id)",
    #             {'id': 6666}
    #         )
    #     insertmany:
    #         self.insert(
    #             "insert into test(id) values(:id)",
    #             [{'id': 6666}, {'id': 8888}]
    #         )
    #     @num:
    #         批量插入时定义一次插入的数量，默认10000
    #     """
    #     length = len(args)
    #     count = 0
    #     try:
    #         if (args and not isinstance(args, dict) and
    #                 isinstance(args[0], (tuple, list, dict))):
    #             for i in range(0, length, num):
    #                 rs = self.execute(sql, args[i:i + num])
    #                 count += rs.rowcount
    #         else:
    #             rs = self.execute(sql, args)
    #             count = rs.rowcount
    #     except DatabaseError as reason:
    #         self.rollback()
    #         # if 0:
    #         #     pass
    #         if 'ORA-12899' in str(reason):
    #             self._modify_field_size(reason)
    #             count += self.insert(sql, args, num)
    #         else:
    #             if (args and not isinstance(args, dict) and
    #                     isinstance(args[0], (tuple, list, dict))):
    #                 if reduce_num(num, length) <= 10 or length <= 10:
    #                     log.error("SQL EXECUTEMANY ERROR EXECUTE EVERYONE")
    #                     for record in args[i:i + num]:
    #                         self.insert(sql, record)
    #                 else:
    #                     self.insert(
    #                         sql, args[i:i + num],
    #                         num=reduce_num(num, length))
    #             else:
    #                 log.error(
    #                     'SQL EXECUTE ERROR\n%s\n%s' %
    #                     (sql, args))
    #                 log.error(reason)
    #                 sys.exit()
    #     return count
    def insert(self, sql, args=[], num=10000):
        rs = self.execute(sql, args, num)
        return rs.rowcount

    def rollback(self):
        self.session.rollback()

    def commit(self):
        self.session.commit()

    def close(self):
        """
        关闭数据库连接
        """
        if self._connect is not None:
            self.session.close()

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


def db_test():
    db_uri = "oracle+cx_oracle://jwdn:password@local:1521/xe"
    with Connection(db_uri, echo=True) as db:
        sql = "insert into test(foo,bar) values(:1,:1)"
        print(db.insert(sql, [('aaa', 'bbb'), ('aaa', 'bbb'), ('aaa', 'bbb')]))
        sql = "insert into test(foo,bar) values(:a,:b)"
        print(db.insert(sql, {'a': 'AAA', 'b': 'BBB'}))
        # print(db.delete_repeat('test', 'id', 'dtime'))
        # db.merge('test', {'foo': '1', 'id': 2222}, ['foo', 'id'], 'foo')


if __name__ == "__main__":
    db_test()
