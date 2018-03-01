import pymysql


class Database:

    def __init__(self, host, port, username, password, database_name):
        """
        构造函数
        :param host: 数据库host
        :param port: 数据库端口
        :param username: 数据库用户名
        :param password: 数据库密码
        :param database_name: 数据库名称
        """
        self.url = host
        self.port = port
        self.username = username
        self.password = password
        self.database_name = database_name
        self.database = pymysql.connect(host=host, port=port, username=username, password=password,
                                        database_name=database_name)

    def __del__(self):
        """
        析构函数，关闭数据库连接
        """
        database = self.database
        database.close()

    def create_table(self, table_name, sql):
        """
        创建数据库表格
        :param table_name: 数据库表名
        :param sql: 待执行的SQL语句
        """
        cursor = self.database.cursor()
        cursor.execute("DROP TABLE IF EXISTS " + table_name)
        cursor.execute(sql)

    def insert(self, sql):
        """
        数据库插入操作
        :param sql: 待执行的SQL语句
        """
        cursor = self.database.cursor()
        print(sql)
        try:
            cursor.execute(sql)
            self.database.commit()
            print("插入成功")
        except Exception as E:
            self.database.rollback()
            print(E)
            print("插入失败")

    def select(self, sql):
        """
        数据库查询操作
        :param sql: 待执行的SQL语句
        :return: 查询结果
        """
        cursor = self.database.cursor()
        try:
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
        except:
            print("Error: unable to fetch data")

    def update(self, sql):
        """
        数据库更新操作
        :param sql: 待执行的SQL语句
        """
        cursor = self.database.cursor()
        try:
            cursor.execute(sql)
            self.database.commit()
        except:
            self.database.rollback()

    def delete(self, sql):
        """
        数据库删除操作
        :param sql: 待执行的SQL语句
        """
        cursor = self.database.cursor()
        try:
            cursor.execute(sql)
            self.database.commit()
        except:
            self.database.rollback()