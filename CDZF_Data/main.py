# -*- coding: utf-8 -*-
# 作者: JianNoo
# 功能描述: 主函数
# 网址: http://www.cddata.gov.cn/odweb/catalog/index.htm?org_code=e39781f3299e422d96a8408f51b25b3d
import os
import sys
from CDZF_Data import create_database, conversion_data


def mian():
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    create_database.create_db().main()  # 建库和建表
    os.system('scrapy crawl revoke')  # 爬取成都市个体工商户吊销数据
    os.system('scrapy crawl cancel')  # 爬取成都市个体工商户注销数据
    os.system('scrapy crawl survivla')  # 爬取成都市个体工商户存活数据
    conversion_data.Conversion().main()  # 去重保存到sqlite


if __name__ == '__main__':
    mian()
