# pydbclib: Python Database Connectivity Lib

pydbclib is a database utils for python

## Installation:
```shell script
pip install 'pydbclib>=2.0'
```

## A Simple Example:

```python
import pydbclib
with pydbclib.connect("sqlite:///:memory:") as db:
    db.execute('create table foo(a integer, b varchar(20))')
    db.get_table("foo").insert_one({"a": 1, "b": "one"})
    print(db.get_table("foo").find_one({"a": 1}))
```

## 接口使用演示

```python
import pydbclib
db = pydbclib.connect("sqlite:///:memory:")
# # 数据库基础操作 # #
# 单个插入和批量插入，结果返回影响行数
record = {"a": 1, "b": "one"}
db.write("insert into foo(a,b) values(:a,:b)", record)
db.write_many("insert into foo(a,b) values(:a,:b)", [record]*10)
# 查询操作，返回生成器字典集合
db.read("select * from foo")
# 参数to_dict=false，返回生成器元祖集合
db.read("select * from foo", to_dict=False)
# 查询结果只返回一条记录：{"a": 1, "b": "one"}
db.read_one("select * from foo")

# # 应对表的快捷操作 # #
# 插入单条和插入多条，输入参数字典的键值必须和表中字段同名
db.get_table("foo").insert_one({"a": 1, "b": "one"})
db.get_table("foo").insert_many([{"a": 1, "b": "one"}])
# 查询字段a=1所有记录
db.get_table("foo").find({"a": 1})
# 也可以直接写成sql条件表达式，下面几个函数的条件部分都可以写成sql条件表达式
db.get_table("foo").find("a=1")
# 查询字段a=1第一条记录
db.get_table("foo").find_one({"a": 1})
# 将a=1那条记录的b字段值更新为"first"
db.get_table("foo").update({"a": 1}, {"b": "first"})
# 将a=1那条记录删除
db.get_table("foo").delete({"a": 1})

db.commit()
db.close()
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
    db = pydbclib.connection("oracle://user:password@local:1521/xe")
    # 连接mysql
    db = pydbclib.connection("mysql+pyodbc://:@mysqldb")
    # 通过已有engine连接
    from sqlalchemy import create_engine
    engine = create_engine("mysql+pymysql://user:password@localhost:3306/test")
    db = pydbclib.connection(driver=engine)



### 详细使用文档 

https://blog.csdn.net/li_yatao/article/details/105185444
