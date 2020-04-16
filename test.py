# -*- coding: utf-8 -*-
"""
@time: 2020/3/26 11:42 上午
@desc:
"""
import unittest
from collections.abc import Iterator

import pydbclib


class TestDataBase(unittest.TestCase):

    def execute(self, db):
        db.execute('CREATE TABLE foo (a integer, b varchar(20))')
        try:
            self.action(db)
        finally:
            db.execute('DROP TABLE foo')

    def action(self, db):
        record = {"a": 1, "b": "1"}
        db.get_table("foo").insert_one(record)
        self.assertEqual(db.get_table("foo").find_one({"a": 1}), record)
        r = db.get_table("foo").find({"a": 1})
        self.assertEqual(r.limit(1), [record])
        self.assertIsInstance(r, Iterator)
        record.update(a=2)
        self.assertEqual(db.get_table("foo").insert([record for i in range(10)]), 10)
        self.assertEqual(db.get_table("foo").update({"a": 2}, {"b": "2"}), 10)
        record.update(b="2")
        r = db.get_table("foo").find({"a": 2})
        self.assertEqual(r.limit(5), [record for _ in range(5)])
        self.assertIsInstance(r, Iterator)
        self.assertEqual(db.get_table("foo").delete({"a": 2}), 10)
        r = db.get_table("foo").find({"a": 2})
        self.assertIsInstance(r, Iterator)
        self.assertEqual(list(r), [])
        self.assertEqual(db.get_table("foo").find_one({"a": 2}), {})

    def test_sqlalchemy_sqlite_memory(self):
        with pydbclib.connect("sqlite:///:memory:") as db:
            self.execute(db)

    def test_sqlite(self):
        with pydbclib.connect(":memory:", driver="sqlite3") as db:
            self.execute(db)

    def test_mysql(self):
        with pydbclib.connect("mysql+pymysql://test:test@localhost:3306/test") as db:
            self.execute(db)

    def test_pymysql(self):
        with pydbclib.connect(driver="pymysql", database="test", user="test", password="test") as db:
            self.execute(db)


if __name__ == '__main__':
    unittest.main()
