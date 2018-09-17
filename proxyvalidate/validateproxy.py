import os
from concurrent.futures import ProcessPoolExecutor
import time
import datetime
import pymongo
import urllib3

MONGO_URL = 'mongodb://10.30.16.206:27017'
MONGO_DB = 'proxydb'
MONGO_USER = 'proxy'
MONGO_PWD = 'proxy123' 

TIMEOUT = 4.0

'''
    通过多进程进行代理ip地址的验证； 
    1. 通过爬虫抓取的记录放在 proxypool中 
    2. 多进程验证 ， 验证通过的则插入 proxypoolnow 
    3. 验证不通过则 ，则从proxypool、proxypoolnow删除 ， 插入到proxyhistory中  
    4. 使用时从 proxypoolnow中取最近时间戳的记录使用 。
'''
## 用于分发 
class Dispatcher(object):
    def __init__(self):
        self.cpus = os.cpu_count() + 1
        self.execpool = ProcessPoolExecutor(self.cpus)
        print("Dispatcher init... ")
        self.client = pymongo.MongoClient(MONGO_URL)
        self.db = self.client[MONGO_DB]
        self.db.authenticate(MONGO_USER, MONGO_PWD)
        self.table = self.db['proxypool']
        self.tablenow = self.db['proxypoolnow']
        self.tabledel = self.db['proxyhistory']
        self.validate = ValidateProcess()

    ## 排序status ， 不处理透明的ip代理 ，只处理匿名的 {'anonymous':'A'} 
    ## 增加 对国外ip的单独处理  
    def findstatus(self):
        res = self.table.find({'$or' : [{'anonymous': 'A'}, {'anonymous': {'$regex' : '^W'}}]}).distinct('status')
        res.sort()
        res.reverse()
        print("当前的status ：" , res)
        return res

    # 5. 在callback中，对无法满足的响应时间的进行删除处理 
    # 增加对国外的处理  
    def done_callback(self, resfuture):
        try:
            res = resfuture.result()
            #print(res)
            if (res.get('result') < 1) :
                ip =  res.get('record').get('ip')
                port = res.get('record').get('port')
                print("DELETE ",ip , port )
                # self.table.delete_one({'_id': res.get('record').get('_id')})
                self.table.delete_many({'ip': ip, 'port': port})
                self.tabledel.save(res.get('record'))
                self.tablenow.delete_one({'url': res.get("purl")})
            else:
                print("INSERT ")
                # 无则新插入， 有则更新 
                self.tablenow.replace_one({'url': res.get("purl")}, {'url': res.get("purl"), 'tm': datetime.datetime.now(), 'anonymous': res.get('record').get('anonymous') } , upsert=True)
        except Exception as e:
            print("Error in futrue callback:", e)
        
    def start(self):
        # dispatcher  
        while(True):
            # 1. 获取当前表中的status 
            statuss = self.findstatus()
            # 2. 循环进行status处理 降序开始处理  
            for s in statuss:
                # 3. 对status进行 +1 update  
                # Pm. 当status的+1 操作到最大值时会怎么样  
                self.table.update_many({'$and' : [{'status':s }, {'$or' : [{'anonymous': 'A'}, {'anonymous': {'$regex' : '^W'}}]}]}, {'$inc': {'status': 1}}) 
                rlist = self.table.find(filter={'$and' : [{'status':s+1 }, {'$or' : [{'anonymous': 'A'}, {'anonymous': {'$regex' : '^W'}}]}]}, projection=['ip', 'port', 'anonymous', 'status', 'http', 'kinddesc'])
                print('待发送列表长度：' , rlist.count())

                for p in rlist:
                    # 4. 对每条记录 循环分配到线程池处理  
                    self.validate.validate(p, self.done_callback, self.execpool)
            
            # 休眠30s继续循环进行处理 
            # break
            time.sleep(30)


class ValidateProcess(object):
    def validate(self, proxy, callback_fn, execpool):
        future = execpool.submit(self.dovalidateproxy, proxy)
        future.add_done_callback(callback_fn)

    # 主验证函数  
    def dovalidateproxy(self, proxy):
        # print("validate:" , proxy)
        purl = proxy.get('http').lower() + "://" + proxy.get('ip') + ":" + proxy.get("port")
        print(purl)
        header = {
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Upgrade-Insecure-Requests': 1,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.79 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9'
        }

        # 增加国外的判断  
        anonymous = proxy.get('anonymous')

        ulib = urllib3.ProxyManager(proxy_url=purl, num_pools=1, headers=header)
        try:
            if anonymous == 'A':
                ulib.request('GET', "http://www.baidu.com", timeout=TIMEOUT ) 
            else:
                ulib.request('GET', "https://www.google.com", timeout=TIMEOUT ) 
        except urllib3.exceptions.HTTPError :
            print("Not avaliable proxy ", purl)
            # delete item 
            return {'result': 0 ,'purl':purl, 'record': proxy}
        
        print("可用proxy：", purl)
        return {'result': 1 , 'purl':purl, 'record': proxy}

if __name__ == '__main__':
    d = Dispatcher()
    d.start()