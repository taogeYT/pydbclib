## pydbclib: Python Database Connectivity Lib
pydbclib is a database utils for python

### Installation:
    pip install pydbclib

#### usage:

    from pydbclib import connection
    with connection("test.db", driver="sqlite3") as db:
        db.ddl('create table test (id varchar(4) primary key, name varchar(10))')
        db.write("insert into test (id, name) values(:id, :name)", {'id':1, 'name':'test'}) # 返回行数
        db.write_by_dict('test', {'id': 2, 'name': 'test2'}) # 返回行数
        db.read("select * from test limit :1", [10]) # 返回元祖集合
        db.read_dict("select * from test") # 返回字典集合
        db.ddl('drop table test')
    
    # udf扩展
    from pydbclib import Connection  
    class MyUDF(Connection):
        def total_data(self, table):  
            return self.read("select count(*) from :1", table)

    with MyUDF("oracle://lyt:lyt@local:1521/xe") as db:
        count = db.total_data('test')
        print("test表的总数量为:", count)


#### 常用数据库连接  
Common Driver  

    # 连接oracle
    db = connection('jwdn/password@local:1521/xe', driver="cx_Oracle")
    # 通过odbc方式连接
    db = connection('DSN=mydb;UID=root;PWD=password', driver="pyodbc")  

Sqlalchemy Driver

    # 连接oracle
    db = connection("oracle://jwdn:password@local:1521/xe")
    # 连接mysql
    db = connection("mysql+pyodbc://:@mysqldb")
