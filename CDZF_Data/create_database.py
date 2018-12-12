# -*- coding: utf-8 -*-
# 作者: JianNoo
# 功能描述: 创建数据库和创建数据表
# 网址: http://www.cddata.gov.cn/odweb/catalog/index.htm?org_code=e39781f3299e422d96a8408f51b25b3d
import pymysql
from CDZF_Data import settings


class create_db(object):
    def __init__(self):
        self.__config = {  # 从settings文件获取数据库连接配置信息
            'host': settings.MYSQL_HOST,
            'port': settings.MYSQL_PORT,
            'username': settings.MYSQL_USER,
            'password': settings.MYSQL_PASSWORD,
            'database': settings.MYSQL_DBNAME,
            'charset': settings.MYSQL_CHARSET
        }

    def connect_mysql(self):
        db = pymysql.connect(  # 连接数据库
            host=self.__config['host'],
            port=self.__config['port'],
            user=self.__config['username'],
            passwd=self.__config['password'],
            charset=self.__config['charset']
        )
        return db

    def create_db(self):
        db = self.connect_mysql()
        cursor = db.cursor()
        cursor.execute('show databases')
        rows = cursor.fetchall()
        for row in rows:
            if self.__config['database'] in row[0]:  # 判断数据库是否存在，存在则关闭程序
                cursor.close()
                db.close()
                print("数据库名已存在，请修改！")
                import sys
                sys.exit()
        cursor.execute('CREATE DATABASE IF NOT EXISTS %s default character set utf8 COLLATE utf8_general_ci' % self.__config['database'])
        # 不存在则创建
        print("数据库创建成功！")
        cursor.close()
        db.close()

    def create_table(self):
        db = self.connect_mysql()
        cursor = db.cursor()
        cursor.execute('use ' + self.__config['database'])
        for key, value in settings.TABLE_INFO.items():
            for table_name, field_name in value.items():  # 创建数据表
                sql = """CREATE TABLE `%s` (
                          `id` int(11) NOT NULL AUTO_INCREMENT,
                          `credit_code` varchar(1000) DEFAULT NULL,
                          `name` varchar(1000) DEFAULT NULL,
                          `address` varchar(1000) DEFAULT NULL,
                          `operator` varchar(1000) DEFAULT NULL,
                          `scope` varchar(1000) DEFAULT NULL,
                          `start_date` varchar(1000) DEFAULT NULL,
                          `end_date` varchar(1000) DEFAULT NULL,
                          `%s` varchar(1000) DEFAULT NULL,
                          PRIMARY KEY (`id`)
                          )DEFAULT CHARSET=utf8;""" % (table_name, field_name)
                cursor.execute(sql)
                print('数据库%s表创建成功！' % table_name)

    def main(self):
        self.create_db()  # 创建数据库
        self.create_table()  # 创建数据表


if __name__ == '__main__':
    create_db().main()

