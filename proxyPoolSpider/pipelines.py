# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
import datetime

class MongoPipeline(object):

	def __init__(self, mongurl, mongdb, monguser, mongpwd):
		self.mongourl = mongurl
		self.mongodb = mongdb
		self.mongouser = monguser
		self.mongopwd = mongpwd

	@classmethod
	def from_crawler(cls, crawler):
		return cls(mongurl=crawler.settings.get('MONGO_URL'), mongdb=crawler.settings.get('MONGO_DB'), \
		monguser=crawler.settings.get("MONGO_USER"), mongpwd=crawler.settings.get('MONGO_PWD'))

	def open_spider(self, spider):
		print("In Mongo pipeline:", self.mongourl, self.mongodb, self.mongouser, self.mongopwd)
		self.client = pymongo.MongoClient(self.mongourl)
		
		self.db = self.client[self.mongodb]
		self.db.authenticate(self.mongouser , self.mongopwd)

	def close_spider(self, spider):
		self.client.close()

	## 插入数据，根据ip 端口 判断是否已经存在，存在则不插入 
	## 后续校验端口是否存活， 不存活则直接物理删除之 ；
	def process_item(self, item, spider):
		table = self.db['proxypool']
		# timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
		datalist = []
		total = 0
		existcount = 0

		for res in item['reslist']:
			##校验是否已经存在 
			count = table.find({'ip':res.get('ip') , 'port': res.get('port')}).count()
			if (count > 0):
				existcount += 1
				print("库中已存在，不再插入")
				continue
			data = {}
			total += 1
			data.update(res)
			data['domain'] = item['domain']
			data['kinddesc'] = item['kinddesc']
			data['status'] = 0
			data['timestamp'] = datetime.datetime.now()
			data['rmrk'] = ''
			datalist.append(data)
		print("准备保存数据insertmany：", item['domain'], len(datalist) , existcount, total)

		self.saveCrawDetails(item['url'], item['kinddesc'], total, len(datalist), existcount)

		if (len(datalist) == 0):
			print("解析数据为0 ，不入库")
			return item

		table.insert_many(datalist)
		
		print("插入数据条数：", total)
		return item

	def saveCrawDetails(self, url, kinddesc, total, addcount, existcount):
		# 记录每个网页的抓取情况 日志  
		timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S') 

		detail = self.db['crawdetail']
		detail.insert_one({'url':url, 'kinddesc':kinddesc , 'total':total, 'addcount':addcount, 'existcount':existcount, 'timestamp':timestamp})


class ProxypoolspiderPipeline(object):
    def process_item(self, item, spider):
        return item
