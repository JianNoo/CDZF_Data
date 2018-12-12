# -*- coding: utf-8 -*-
# 作者: JianNoo
# 功能描述: 保存爬取到的数据
# 网址: http://www.cddata.gov.cn/odweb/catalog/index.htm?org_code=e39781f3299e422d96a8408f51b25b3d
# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html
import scrapy


class Survivla_Item(scrapy.Item):
    # define the fields for your item here like:
    credit_code = scrapy.Field()
    name = scrapy.Field()
    address = scrapy.Field()
    operator = scrapy.Field()
    scope = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    establish_date = scrapy.Field()


class Revoke_Item(scrapy.Item):
    # define the fields for your item here like:
    credit_code = scrapy.Field()
    name = scrapy.Field()
    address = scrapy.Field()
    operator = scrapy.Field()
    scope = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    revoke_date = scrapy.Field()


class Cancel_Item(scrapy.Item):
    # define the fields for your item here like:
    credit_code = scrapy.Field()
    name = scrapy.Field()
    address = scrapy.Field()
    operator = scrapy.Field()
    scope = scrapy.Field()
    start_date = scrapy.Field()
    end_date = scrapy.Field()
    cancel_date = scrapy.Field()

