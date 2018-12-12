# -*- coding: utf-8 -*-
# 作者: JianNoo
# 功能描述: 将SQLServer数据去重后转换成Sqlite数据
# 网址: http://www.cddata.gov.cn/odweb/catalog/index.htm?org_code=e39781f3299e422d96a8408f51b25b3d
import pymysql
import sqlite3
import os
from CDZF_Data import settings
import pandas as pd


class Conversion(object):
    def __init__(self):
        self.__config = {  # 从settings文件获取数据库连接配置信息
            'host': settings.MYSQL_HOST,
            'port': settings.MYSQL_PORT,
            'username': settings.MYSQL_USER,
            'password': settings.MYSQL_PASSWORD,
            'database': settings.MYSQL_DBNAME,
            'charset': settings.MYSQL_CHARSET
        }
        self.path = os.path.dirname(os.path.abspath(__file__)) + '\Data'  # 数据存储路径

    def create_folder(self):  # 创建文件夹，存储sqlite数据库
        # 判断路径是否存在
        isExists = os.path.exists(self.path)
        if not isExists:
            # 如果不存在则创建目录
            os.makedirs(self.path)

    def connect_mysql(self):
        db = pymysql.connect(  # 连接数据库
            host=self.__config['host'],
            port=self.__config['port'],
            user=self.__config['username'],
            passwd=self.__config['password'],
            db=self.__config['database'],
            charset=self.__config['charset']
        )
        return db

    def storage_data(self, c_table_name, table_name, field_name):
        db = self.connect_mysql()  # 获取数据库对象
        # sql查询语句
        sql = """SELECT credit_code, `name`, address, operator, scope, start_date, end_date, %s
                 FROM %s""" % (field_name, table_name)
        df = pd.read_sql(sql, db)  # 使用pandas获取数据
        df1 = df.drop_duplicates()  # 去重
        path = self.path + '\\' + '成都市个体工商户数据.db'  # Sqlite数据库存储路径
        conn = sqlite3.connect(path)  # 创建sqlite数据库
        df1.to_sql(c_table_name, conn, 'sqlite', if_exists='append', index=False)  # 将数据存储进Sqlite
        db.close()  # 关闭数据库
        conn.close()  # 关闭数据库

    def main(self):  # 主函数
        self.create_folder()  # 创建文件夹
        for c_table_name, table_info in settings.TABLE_INFO.items():
            for table_name, field_name in table_info.items():
                self.storage_data(c_table_name, table_name, field_name)


if __name__ == '__main__':
    Conversion().main()