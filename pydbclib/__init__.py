# -*- coding: utf-8 -*-
"""
    Python Database Connectivity lib
"""
from pydbclib.database import Database
from pydbclib.drivers import CommonDriver, SQLAlchemyDriver

__author__ = "liyatao"
__version__ = '2.1.7'


def connect(*args, **kwargs):
    driver = kwargs.get("driver", "sqlalchemy")
    if isinstance(driver, str):
        driver_class = {"sqlalchemy": SQLAlchemyDriver}.get(driver.lower(), CommonDriver)
    elif hasattr(driver, "cursor"):
        driver_class = CommonDriver
    else:
        driver_class = SQLAlchemyDriver
    return Database(driver_class(*args, **kwargs))
