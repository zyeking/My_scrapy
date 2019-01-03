# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from pymongo import MongoClient


class GraduationProjectPipeline(object):
    # 实现保存到Mongo数据库的类
    # collection = "house"  # mongo数据库collection的名字
    collection = "demo"  # mongo数据库collection的名字

    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        # self.db_user = db_user
        # self.db_pass = db_pass

    @classmethod
    def from_crawler(cls, crawler):
        # scrapy为我们访问settings提供了这样的一个方法,
        # 这里我们需要从settings.py中取得数据库的URI和数据库名
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            db_name=crawler.settings.get('DB_NAME'),
            # db_user=crawler.settings.get('DB_USER'),
            # db_pass=crawler.settings.get('DB_PASS')
            )


    def open_spider(self, spider):  # 爬虫启动时调用,连接到数据库
        self.client = MongoClient(self.mongo_uri)
        self.ftx = self.client[self.db_name]
        # self.FangTianXia.authenticate(self.db_user, self.db_pass)


    def close_spider(self, spider):  # 爬虫关闭时调用,关闭数据库连接
        self.client.close()

    def process_item(self, item, spider):
        self.ftx['demo'].insert(dict(item))
        return item
