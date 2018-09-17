
import pymongo
import datetime
import proxyPoolSpider.settings


class DbUtils(object):

	def __init__(self):
		print("in DbUtils __init__ ")
		self.mongourl = proxyPoolSpider.settings.MONGO_URL
		self.mongodb = proxyPoolSpider.settings.MONGO_DB
		self.mongouser = proxyPoolSpider.settings.MONGO_USER
		self.mongopwd = proxyPoolSpider.settings.MONGO_PWD
		print(self.mongourl, self.mongodb, self.mongouser, self.mongopwd)
		self.open_spider()
		

	@classmethod
	def from_crawler(cls, crawler):
		print("in DbUtils from_crawler ")
		# return cls(mongurl=crawler.settings.get('MONGO_URL'), mongdb=crawler.settings.get('MONGO_DB'))
		return cls()

	def open_spider(self):
		print("in DbUtils open_spider ")
		self.client = pymongo.MongoClient(self.mongourl)
		self.db = self.client[self.mongodb]
		self.db.authenticate(self.mongouser, self.mongopwd)
		self.tablenow = self.db['proxypoolnow']

	def close_spider(self):
		self.client.close()

	def process_item(self, item, spider):
		return item

	# 记录爬取异常的日志 
	def saveCrawLog(self, url, kinddesc, status, records):
		# 只记录 error 日志  
		timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S') 

		errortable = self.db['crawlog']
		errortable.insert_one({'url':url, 'status':status, 'kinddesc':kinddesc , 'records':records, 'timestamp':timestamp})

	# 记录网页抓取详情，抓取的ip个数，已存在的个数  
	def saveCrawDetails(self, url, kinddesc, total, addcount, existcount):
		# 记录每个网页的抓取情况 日志  
		timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S') 

		detail = self.db['crawdetail']
		detail.insert_one({'url':url, 'kinddesc':kinddesc , 'total':total, 'addcount':addcount, 'existcount':existcount, 'timestamp':timestamp})

	## 从proxypoolnow表中取最新的num个代理，以列表的形式返回 
	def findProxypool(self, num):
		res = self.tablenow.find().sort('tm',pymongo.DESCENDING).limit(num)
		#return self.tablenow.find(sort=('tm',pymongo.DESCENDING), limit=30)
		reslist = []
		for r in res:
			reslist.append(r)

		return reslist