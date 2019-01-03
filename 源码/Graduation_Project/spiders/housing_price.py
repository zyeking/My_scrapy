# -*- coding: utf-8 -*-
import scrapy
from Graduation_Project.items import GraduationProjectItem
from scrapy import Spider
import requests
from scrapy import Selector
import time


class HousingPriceSpider(scrapy.Spider):
    name = 'housing_price'
    # allowed_domains = ['http://gz.zu.fang.com/']
    start_urls = ['http://gz.zu.fang.com']

    def parse(self, response):
        loc_list = []
        dd = response.css('.search-list.clearfix')

        location = dd.css('dd a::attr(href)').extract()[1:12]  # 获取广州十一个区域的href
        for i in location:
            url = 'http://gz.zu.fang.com' + i
            loc_list.append(url)

        for loc in loc_list:  # 分别获取每一个区域所有的房源信息href
            for j in range(1, 101):
                url = loc + 'i3' + str(j)
                yield scrapy.Request(url, callback=self.parse_housing)
        time.sleep(2)

    def parse_housing(self, response):
        try:
            # 获取每一条租房信息的详细页
            ps = response.css('.info.rel')
            for p in ps:
                item = GraduationProjectItem()
                hrefs = p.css('.title a::attr(href)').extract()[0]
                url = 'http://gz.zu.fang.com' + hrefs
                yield scrapy.Request(url, callback=self.house_detalis)
            # print(url)
        except:
            return 'whoops!'


    def house_detalis(self, response):
        item = GraduationProjectItem()  # 实例化item类
        dd = response.css('.tab-cont.clearfix')

        title = dd.css('.title ::text').extract()  # 名称
        price = dd.css('.trl-item.sty1 i::text').extract_first()  # 价格
        lease_mode = dd.css(
            'div.tab-cont-right > div:nth-child(3) > div.trl-item1.w146 > div.tt ::text').extract_first()  # 出租方式
        apartment = dd.css(
            'div.tab-cont-right > div:nth-child(3) > div.trl-item1.w182 > div.tt ::text').extract_first()  # 户型...
        area = dd.css(
            'div.tab-cont-right > div:nth-child(3) > div.trl-item1.w132 > div.tt ::text').extract_first()  # 建筑面积
        orientation = dd.css(
            'div.tab-cont-right > div:nth-child(4) > div.trl-item1.w146 > div.tt ::text').extract_first()  # 朝向
        floor = dd.css(
            'div.tab-cont-right > div:nth-child(4) > div.trl-item1.w182 > div.tt ::text').extract_first()  # 楼层
        renovation = dd.css(
            'div.tab-cont-right > div:nth-child(4) > div.trl-item1.w132 > div.tt ::text').extract_first()  # 装修
        quarters = dd.css('.trl-item2.clearfix a::text').extract()  # 小区

        # 生成字典
        item['title'] = title
        item['price'] = price
        item['lease_mode'] = lease_mode
        item['apartment'] = apartment
        item['area'] = area
        item['orientation'] = orientation
        item['floor'] = floor
        item['renovation'] = renovation
        item['quarters'] = quarters
        yield item
