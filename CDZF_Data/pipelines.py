# -*- coding: utf-8 -*-
# 作者: JianNoo
# 功能描述: 将数据异步存储进数据库
# 网址: http://www.cddata.gov.cn/odweb/catalog/index.htm?org_code=e39781f3299e422d96a8408f51b25b3d
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import datetime, os
import pymysql
import pymysql.cursors
from twisted.enterprise import adbapi
import copy


class Survivla_MysqlTwistPipeline(object):  # 异步

    @classmethod
    def from_settings(cls, settings):  # 使用scrapy内置函数获取数据库配置信息
        adbparams = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            charset=settings['MYSQL_CHARSET'],
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True,
            )
        # 这是链接数据库的另一种方法，在settings中写入参数
        dbpool = adbapi.ConnectionPool('pymysql', **adbparams)  # 异步
        return cls(dbpool)

    def __init__(self, dbpool):
        self.dbpool = dbpool

    def process_item(self, item, spider):
        # 使用twiest将mysql插入变成异步
        asynItem = copy.deepcopy(item)
        # 使用异步时，需要使用copy的深度复制
        query = self.dbpool.runInteraction(self.do_insert, asynItem)
        # 因为异步 可能有些错误不能及时爆出
        query.addErrback(self.handle_error, asynItem)
        return item

    # 处理异步的异常
    def handle_error(self, failure, item):
        with open('survivla_error.log', 'a') as f:
            time = str(datetime.datetime.now())[:-7]  # 生成时间
            t = os.linesep
            s = time + ' : ' + '插入数据失败，原因：{}，错误对象：{}'.format(failure, item)
            f.write(s + t + t)  # 写入日志

    def do_insert(self, cursor, item):
        # sql语句
        insert_sql = """
                INSERT INTO survivla_data(credit_code, `name`, address, operator, scope, start_date, end_date, establish_date)
                VALUES('%s','%s','%s', '%s', '%s','%s','%s','%s')
        """ % (
            item['credit_code'],
            item['name'],
            item['address'],
            item['operator'],
            item['scope'],
            item['start_date'],
            item['end_date'],
            item['establish_date'],)
        cursor.execute(insert_sql)  # 执行sql语句
        return item


class Revoke_MysqlTwistPipeline(object):  # 异步
    @classmethod
    def from_settings(cls, settings):  # 使用scrapy内置函数获取数据库配置信息
        adbparams = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            charset=settings['MYSQL_CHARSET'],
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True,
            )
        # 这是链接数据库的另一种方法，在settings中写入参数
        dbpool = adbapi.ConnectionPool('pymysql', **adbparams)  # 异步
        return cls(dbpool)

    def __init__(self, dbpool):
        self.dbpool = dbpool

    def process_item(self, item, spider):
        # 使用twiest将mysql插入变成异步
        asynItem = copy.deepcopy(item)
        # 使用异步时，需要使用copy的深度复制
        query = self.dbpool.runInteraction(self.do_insert, asynItem)
        # 因为异步 可能有些错误不能及时爆出
        query.addErrback(self.handle_error, asynItem)
        return item

    # 处理异步的异常
    def handle_error(self, failure, item):
        with open('revoke_error.log', 'a') as f:
            time = str(datetime.datetime.now())[:-7]  # 生成时间
            t = os.linesep
            s = time + ' : ' + '插入数据失败，原因：{}，错误对象：{}'.format(failure, item)
            f.write(s + t + t)  # 写入日志

    def do_insert(self, cursor, item):
        # sql语句
        insert_sql = """
                INSERT INTO revoke_data(credit_code, `name`, address, operator, scope, start_date, end_date, revoke_date)
                VALUES('%s','%s','%s', '%s', '%s','%s','%s','%s')
        """ % (
            item['credit_code'],
            item['name'],
            item['address'],
            item['operator'],
            item['scope'],
            item['start_date'],
            item['end_date'],
            item['revoke_date'],)
        cursor.execute(insert_sql)  # 执行sql语句
        return item


class Cancel_MysqlTwistPipeline(object):  # 异步

    @classmethod
    def from_settings(cls, settings):  # 使用scrapy内置函数获取数据库配置信息
        adbparams = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            charset=settings['MYSQL_CHARSET'],
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True,
        )
        # 这是链接数据库的另一种方法，在settings中写入参数
        dbpool = adbapi.ConnectionPool('pymysql', **adbparams)  # 异步
        return cls(dbpool)

    def __init__(self, dbpool):
        self.dbpool = dbpool

    def process_item(self, item, spider):
        # 使用twiest将mysql插入变成异步
        asynItem = copy.deepcopy(item)
        # 使用异步时，需要使用copy的深度复制
        query = self.dbpool.runInteraction(self.do_insert, asynItem)
        # 因为异步 可能有些错误不能及时爆出
        query.addErrback(self.handle_error, asynItem)
        return item

    # 处理异步的异常
    def handle_error(self, failure, item):
        with open('cancel_error.log', 'a') as f:
            time = str(datetime.datetime.now())[:-7]  # 生成时间
            t = os.linesep
            s = time + ' : ' + '插入数据失败，原因：{}，错误对象：{}'.format(failure, item)
            f.write(s + t + t)  # 写入日志

    def do_insert(self, cursor, item):
        # sql语句
        insert_sql = """
                INSERT INTO cancel_data(credit_code, `name`, address, operator, scope, start_date, end_date, cancel_date)
                VALUES('%s','%s','%s', '%s', '%s','%s','%s','%s')
        """ % (
            item['credit_code'],
            item['name'],
            item['address'],
            item['operator'],
            item['scope'],
            item['start_date'],
            item['end_date'],
            item['cancel_date'],)
        cursor.execute(insert_sql)  # 执行sql语句
        return item