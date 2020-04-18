# -*- coding: utf-8 -*-
"""
@time: 2020/4/13 11:28 下午
@desc:
"""
import itertools


class Records(object):

    def __init__(self, rows, columns, as_dict):
        self._rows = rows
        self.columns = columns
        self.to_dict = as_dict
        self._limit_num = None

    def __iter__(self):
        return self

    def next(self):
        return next(self._rows)

    __next__ = next

    def map(self, function):
        self._rows = (function(r) for r in self._rows)
        return self

    def rename(self, mapper):
        """
        字段重命名
        """
        def function(record):
            if isinstance(record, dict):
                return {mapper.get(k, k): v for k, v in record.items()}
            else:
                return dict(zip(mapper, record))
        return self.map(function)

    def limit(self, num):
        self._rows = (r for i, r in enumerate(self._rows) if i < num)
        return self

    def get_one(self):
        r = self.get(1)
        return r[0] if len(r)>0 else None

    def get(self, num):
        return [i for i in itertools.islice(self._rows, num)]

    def get_all(self):
        return [r for r in self._rows]

    def to_df(self):
        import pandas
        # if self.columns:
        #     return pandas.DataFrame(self, columns=self.columns)
        # else:
        #     return pandas.DataFrame(self)
        if self.to_dict:
            return pandas.DataFrame.from_records(self)
        else:
            return pandas.DataFrame.from_records(self, columns=self.columns)
