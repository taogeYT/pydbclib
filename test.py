# -*- coding: utf-8 -*-
"""
@time: 2020/3/26 11:42 上午
@desc:
"""
import unittest
import sqlite3
from collections.abc import Iterator

from sqlalchemy import create_engine

from pydbclib import connect


class TestConnect(unittest.TestCase):

    def test_Common_driver(self):
        with connect(":memory:", driver="sqlite3") as db:
            db.execute("select 1")
        con = sqlite3.connect(":memory:")
        with connect(driver=con) as db:
            db.execute("select 1")

    def test_sqlalchemy_driver(self):
        with connect("sqlite:///:memory:") as db:
            db.execute("select 1")
        engine = create_engine("sqlite:///:memory:")
        with connect(driver=engine) as db:
            db.execute("select 1")


class TestDataBase(unittest.TestCase):
    db = None
    record = {"a": 1, "b": "1"}

    @classmethod
    def setUpClass(cls):
        cls.db = connect("sqlite:///:memory:")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()

    def setUp(self):
        self.db.execute("CREATE TABLE foo (a integer, b varchar(20))")

    def tearDown(self):
        self.db.rollback()
        self.db.execute('DROP TABLE foo')

    def test_write(self):
        r = self.db.write("insert into foo(a,b) values(:a,:b)", [self.record]*10)
        self.assertEqual(r, 10)

    def test_read(self):
        r = self.db.read("select * from foo")
        self.assertEqual(r.get(1), [])
        self.assertIsInstance(r, Iterator)
        self.db.get_table("foo").insert([self.record]*10)
        r = self.db.read("select * from foo").map(lambda x: {**x, "c": 3})
        self.assertEqual(r.get(1), [{**self.record, "c": 3}])

    def test_read_one(self):
        self.db.get_table("foo").insert([self.record] * 10)
        r = self.db.read_one("select * from foo")
        self.assertEqual(r, self.record)


class TestTable(unittest.TestCase):
    db = None
    table = None
    record = {"a": 1, "b": "1"}

    @classmethod
    def setUpClass(cls):
        cls.db = connect(":memory:", driver="sqlite3")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()

    def setUp(self):
        self.db.execute("CREATE TABLE foo (a integer, b varchar(20))")
        self.table = self.db.get_table("foo")

    def tearDown(self):
        self.db.rollback()
        self.db.execute('DROP TABLE foo')

    def test_insert(self):
        self.assertEqual(self.table.insert(self.record), 1)
        self.assertEqual(self.table.insert([self.record]*10), 10)

    def test_find(self):
        self.assertEqual(self.table.find().first(), None)
        self.table.insert(self.record)
        r = self.table.find({"a": 1}).map(lambda x: {**x, "c": 3})
        self.assertEqual(r.first(), {**self.record, "c": 3})
        self.assertIsInstance(r, Iterator)
        self.assertEqual(self.table.find({"a": 2}).get(1), [])

    def test_find_one(self):
        self.assertEqual(self.table.find_one(), None)
        self.table.insert(self.record)
        self.assertEqual(self.table.find_one(), self.record)

    def test_update(self):
        self.table.insert([self.record]*10)
        self.assertEqual(self.table.update({"a": 1}, {"b": "2"}), 10)
        self.assertEqual(self.table.find({"a": 1}).get(10), [{"a": 1, "b": "2"}]*10)

    def test_delete(self):
        self.table.insert([self.record] * 10)
        self.assertEqual(self.table.delete({"a": 1}), 10)

    def test_to_df(self):
        self.assertTrue(self.table.find({"a": 1}).to_df().empty)
        self.table.insert([self.record]*10)
        df = self.table.find({"a": 1}).limit(1).to_df()
        self.assertEqual(df.loc[0, 'a'], self.record['a'])
        self.assertEqual(df.loc[0, 'b'], self.record['b'])


if __name__ == '__main__':
    unittest.main()
