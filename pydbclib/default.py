"""
常用数据库占位符
列表里没有的可以手动添加进来
也可以在数据库连接配置中手动添加，参数是placeholder，默认'?'
"""
place_holder = {
    "pymysql": "%s",
    "cx_Oracle": ":",
    "pyodbc": "?",
    "sqlite3": "?",
    "impala.dbapi": "?"
}
