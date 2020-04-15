# -*- coding: utf-8 -*-
"""
@time: 2020/4/13 11:28 下午
@desc:
"""
import itertools


class RecordCollection(object):

    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return self

    def next(self):
        try:
            return next(self._rows)
        except StopIteration:
            raise StopIteration('RecordCollection no more data')

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

    __next__ = next

    def limit(self, num):
        return [i for i in itertools.islice(self._rows, num)]


if __name__ == '__main__':
    RecordCollection((i for i in range(10)))
