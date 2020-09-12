# Pydbclib

Pydbclib is Python Database Connectivity Lib, a general database operation toolkit for **Python 3.6+**

## Installation:
```shell script
pip3 install pydbclib
```

## Example:

```python
from pydbclib import connect
# 使用with上下文，可以自动提交，自动关闭连接
with connect("sqlite:///:memory:") as db:
    db.execute('create table foo(a integer, b varchar(20))')
    # 统一使用':[name]'形式的SQL的占位符
    db.execute("insert into foo(a,b) values(:a,:b)", [{"a": 1, "b": "one"}]*4)
    print(db.read("select * from foo").get_one())
    print(db.read("select * from foo").get_all())
    print(db.read("select * from foo").to_df())
    
    # 对表常用操作的封装
    table = db.get_table("foo")
    table.insert([{"a": 2, "b": "two"}]*2)  # 插入两条记录
    table.find({"b": "two"}).get_all()  # 查出b='two'的所有记录
    table.update({"a": 2, "b": "two"}, {"b": "2"})  # 将a=2 and b='two'的所有记录的b字段值更新为'2'
    table.find({"a": 2}).get_all()  # 查出a=2的所有记录
    table.delete({"a": 2})  # 删除a=2的所有记录
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

https://blog.csdn.net/li_yatao/article/details/79685992
