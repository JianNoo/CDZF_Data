# -*- coding: utf-8 -*-
# 作者: JianNoo
# 功能描述: 爬取成都市个体工商户存活数据
# 网址: http://www.cddata.gov.cn/odweb/catalog/index.htm?org_code=e39781f3299e422d96a8408f51b25b3d
from scrapy import Spider, Request, FormRequest
import json
from CDZF_Data.items import Survivla_Item
import re


class SurvivlaSpider(Spider):
    name = 'survivla'
    allowed_domains = ['www.cddata.gov.cn/']
    start_urls = ['http://www.cddata.gov.cn/odweb/catalog/catalogDetail.htm?cata_id=RDINUS43EeimG7Gh9C7_rQ/']
    custom_settings = {  # 修改settings中的Pipeline的值
        'ITEM_PIPELINES': {'CDZF_Data.pipelines.Survivla_MysqlTwistPipeline': 300}
    }
    def __init__(self):
        self.headers = {  # 请求头
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.76 Mobile Safari/537.36'
        }
        self.start_url = 'http://www.cddata.gov.cn/odweb/catalog/catalogDetail.htm?cata_id=RDINUS43EeimG7Gh9C7_rQ'
        self.info_url = 'http://www.cddata.gov.cn/dataanaly/data/CatalogDetail.do?method=GetDataListForGrid'

    def start_requests(self):
        yield Request(self.start_url, headers=self.headers, callback=self.start_parse)

    def start_parse(self, response):  # 获取数据总量
        data_num_info = response.xpath('/html/body/div[4]/div[1]/div/div/div[1]/div[2]/div[2]/span[1]/text()').extract_first()
        data_num = int(data_num_info.split("：")[1]) // 10
        for i in range(1, data_num+1):
            data = {  # 请求数据
                'cata_id': "RDINUS43EeimG7Gh9C7_rQ",
                'conf_type': "2",
                'where': "",
                '_search': "false",
                'nd': "1542337300712",
                'rows': "10",
                'page': str(i),
                'sidx': "",
                'sord': "asc"
            }
            yield FormRequest(self.info_url, headers=self.headers, formdata=data, callback=self.parse_info,
                              dont_filter=True)  # 请求服务器，获取数据

    def parse_info(self, response):  # 解析服务器返回的数据，使用正则表达式去除特殊符号！
        data = json.loads(response.body)
        regex = re.compile("\.|'|\"|\\\\")
        if data:
            info = data.get('rows')
            items = Survivla_Item()
            for iter in info:
                credit_code = iter.get('idno', '')
                if credit_code is None:
                    items['credit_code'] = credit_code
                else:
                    items['credit_code'] = re.sub(regex, ' ', credit_code)

                name = iter.get('pt_name', '')
                if name is None:
                    items['name'] = name
                else:
                    items['name'] = re.sub(regex, ' ', name)

                address = iter.get('address', '')
                if address is None:
                    items['address'] = address
                else:
                    items['address'] = re.sub(regex, ' ', address)

                operator = iter.get('rep_zjhm', '')
                if operator is None:
                    items['operator'] = operator
                else:
                    items['operator'] = re.sub(regex, ' ', operator)

                scope = iter.get('scope', '')
                if scope is None:
                    items['scope'] = scope
                else:
                    items['scope'] = re.sub(regex, ' ', scope)

                start_date = iter.get('op_fro', '')
                if start_date is None:
                    items['start_date'] = start_date
                else:
                    items['start_date'] = re.sub(regex, ' ', start_date)

                end_date = iter.get('op_to', '')
                if end_date is None:
                    items['end_date'] = end_date
                else:
                    items['end_date'] = re.sub(regex, ' ', end_date)

                establish_date = iter.get('est_date', '')
                if establish_date is None:
                    items['establish_date'] = establish_date
                else:
                    items['establish_date'] = re.sub(regex, ' ', establish_date)
                yield items



