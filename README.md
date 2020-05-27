# Pydbclib

Pydbclib is Python Database Connectivity Lib, a database toolkit for **Python 3.6+**

## Installation:
```shell script
pip3 install pydbclib
```

## Example:

```python
from pydbclib import connect
with connect("sqlite:///:memory:") as db:
    db.execute('create table foo(a integer, b varchar(20))')
    # 统一使用’:[name]'形式的SQL的占位符
    db.execute("insert into foo(a,b) values(:a,:b)", [{"a": 1, "b": "one"}]*4)
    r = db.read("select * from foo")
    print(r.to_df())
    table = db.get_table("foo")
    table.insert({"a": 2, "b": "two"})
    r = table.find({"a": 2})
    r.get_one()
```

#### 常用数据库连接示例  
Common Driver  

    # 使用普通数据库驱动连接，driver参数指定驱动包名称
    # 例如pymysql包driver='pymysql',connect函数其余的参数和driver参数指定的包的创建连接参数一致
    # 连接mysql
    db = pydbclib.connect(user="user", password="password", database="test", driver="pymysql")
    # 连接oracle
    db = pydbclib.connect('user/password@local:1521/xe', driver="cx_Oracle")
    # 通过odbc方式连接
    db = pydbclib.connect('DSN=mysqldb;UID=user;PWD=password', driver="pyodbc")  
    # 通过已有驱动连接方式连接
    import pymysql
    con = pymysql.connect(user="user", password="password", database="test")
    db = pydbclib.connect(driver=con)

Sqlalchemy Driver

    # 使用Sqlalchemy包来连接数据库，drvier参数默认为'sqlalchemy'
    # 连接oracle
    db = pydbclib.connect("oracle://user:password@local:1521/xe")
    # 连接mysql
    db = pydbclib.connect("mysql+pyodbc://:@mysqldb")
    # 通过已有engine连接
    from sqlalchemy import create_engine
    engine = create_engine("mysql+pymysql://user:password@localhost:3306/test")
    db = pydbclib.connect(driver=engine)



### 详细使用文档 

https://blog.csdn.net/li_yatao/article/details/105185444
