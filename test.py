# -*- coding: utf-8 -*-
"""
@time: 2020/3/26 11:42 上午
@desc:
"""
import unittest

import pydbclib


class TestDataBase(unittest.TestCase):

    def action(self, db):
        print()
        db.execute('CREATE TABLE foo (a integer, b varchar(20))')
        record = {"a": 1, "b": "1"}
        db.get_table("foo").insert_one(record)
        self.assertEqual(db.get_table("foo").find_one({"a": 1}), record)
        self.assertEqual(list(db.get_table("foo").find({"a": 1})), [record])
        record.update(a=2)
        self.assertEqual(db.get_table("foo").insert_many([record for i in range(10)]), 10)
        self.assertEqual(db.get_table("foo").update({"a": 2}, {"b": "2"}), 10)
        record.update(b="2")
        self.assertEqual(list(db.get_table("foo").find({"a": 2})), [record for _ in range(10)])
        self.assertEqual(db.get_table("foo").delete({"a": 2}), 10)
        self.assertEqual(list(db.get_table("foo").find({"a": 2})), [])
        self.assertEqual(db.get_table("foo").find_one({"a": 2}), {})
        db.execute('DROP TABLE foo')

    def test_sqlalchemy_sqlite_memory(self):
        with pydbclib.connect("sqlite:///:memory:") as db:
            self.action(db)

    def test_sqlite(self):
        with pydbclib.connect(":memory:", driver="sqlite3") as db:
            self.action(db)

    def test_mysql(self):
        with pydbclib.connect("mysql+pymysql://test:test@localhost:3306/test") as db:
            self.action(db)

    def test_pymysql(self):
        with pydbclib.connect(driver="pymysql", database="test", user="test", password="test") as db:
            self.action(db)
