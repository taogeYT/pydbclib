# pydbclib: Python Database Connectivity Lib

pydbclib is a database utils for python

## Installation:
```shell script
pip install 'pydbclib>=2.1'
```

## A Simple Example:

```python
from pydbclib import connect
with connect("sqlite:///:memory:") as db:
    db.execute('create table foo(a integer, b varchar(20))')
    db.get_table("foo").insert_one({"a": 1, "b": "one"})
    r = db.get_table("foo").find_one({"a": 1})
```

## 接口使用演示

```bash
>>> from pydbclib import connect
>>> db = connect("sqlite:///:memory:")
>>> db.execute('create table foo(a integer, b varchar(20))')
# 单个插入和批量插入，结果返回影响行数
>>> db.write("insert into foo(a,b) values(:a,:b)", [{"a": 1, "b": "one"}]*4)
4
>>> db.read_one("select * from foo")
{'a': 1, 'b': 'one'}
>>> db.read("select * from foo").limit(1)
[{'a': 1, 'b': 'one'}]
>>> db.read("select * from foo").to_df()
   a    b
0  1  one
1  1  one
2  1  one
3  1  one

# 插入单条和插入多条，输入参数字典的键值必须和表中字段同名
>>> db.get_table("foo").insert({"a": 1, "b": "one"})
1
>>> db.get_table("foo").find_one({"a": 1})
{'a': 1, 'b': 'one'}
>>> db.get_table("foo").update({"a": 1}, {"b": "first"})
5
>>> db.get_table("foo").find({"a": 1}).limit(2)
[{'a': 1, 'b': 'first'}, {'a': 1, 'b': 'first'}]
>>> db.get_table("foo").delete({"a": 1})
5
>>> db.commit()
>>> db.close()
```

#### 常用数据库连接  
Common Driver  

    # 使用普通数据库驱动连接，driver参数必须指定驱动包名称，如pymysql包参数driver='pymysql',其他参数和driver参数指定的包的连接参数一致
    # 连接mysql
    db = pydbclib.connect(driver="pymysql", user="user", password="password", database="test")
    # 连接oracle
    db = pydbclib.connect('user/password@local:1521/xe', driver="cx_Oracle")
    # 通过odbc方式连接
    db = pydbclib.connect('DSN=mysqldb;UID=user;PWD=password', driver="pyodbc")  
    # 通过已有驱动连接方式连接
    import pymysql
    con = pymysql.connect(user="user", password="password", database="test")
    db = pydbclib.connect(driver=con)

Sqlalchemy Driver

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
