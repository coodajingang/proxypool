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

    ## 排序status ， 不处r.理透明的ip代理 {'anonymous':'A'}
    def findstatus(self):
        res = self.table.find({'anonymous': 'A'}).distinct('status')
        res.sort()
        res.reverse()
        print("当前的status ：" , res)
        return res

    # 5. 在callback中，对无法满足的响应时间的进行删除处理
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
                self.tablenow.replace_one({'url': res.get("purl")}, {'url': res.get("purl"), 'tm': datetime.datetime.now() } , upsert=True)
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
                self.table.update_many({'status':s , 'anonymous': 'A'}, {'$inc': {'status': 1}}) 
                rlist = self.table.find(filter={'status': s+1, 'anonymous': 'A'}, projection=['ip', 'port', 'anonymous', 'status', 'http', 'kinddesc'])
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

        ulib = urllib3.ProxyManager(proxy_url=purl, num_pools=1, headers=header)
        try:
            ulib.request('GET', "http://www.baidu.com", timeout=5.0 )
        except urllib3.exceptions.HTTPError :
            print("Not avaliable proxy ", purl)
            # delete item 
            return {'result': 0 ,'purl':purl, 'record': proxy}
        
        print("可用proxy：", purl)
        return {'result': 1 , 'purl':purl, 'record': proxy}



"""
class CrawlProcess(object):
    '''
    配合进程池进行URL链接爬取及结果解析；
    最终通过crawl方法的complete_callback参数进行爬取解析结果回调
    '''
    def _request_parse_runnable(self, ip, port):
        print('start get web content from: ' + url)
        try:
            headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
            req = request.Request("http://www.baidu.com", headers=headers)
            content = request.urlopen(req).read().decode("utf-8")
            soup = BeautifulSoup(content, "html.parser", from_encoding='utf-8')
            new_urls = set()
            links = soup.find_all("a", href=re.compile(r"/item/\w+"))
            for link in links:
                new_urls.add(urljoin(url, link["href"]))
            data = {"url": url, "new_urls": new_urls}
            data["title"] = soup.find("dd", class_="lemmaWgt-lemmaTitle-title").find("h1").get_text()
            data["summary"] = soup.find("div", class_="lemma-summary").get_text()
        except BaseException as e:
            print(str(e))
            data = None
        return data

    def crawl(self, url, complete_callback, process_pool):
        future = process_pool.submit(self._request_parse_runnable, url)
        future.add_done_callback(complete_callback)


class OutPutProcess(object):
    '''
    配合进程池对上面爬取解析进程结果进行进程池处理存储；
    '''
    def _output_runnable(self, crawl_result):
        try:
            url = crawl_result['url']
            title = crawl_result['title']
            summary = crawl_result['summary']
            save_dir = 'output'
            print('start save %s as %s.txt.' % (url, title))
            if os.path.exists(save_dir) is False:
                os.makedirs(save_dir)
            save_file = save_dir + os.path.sep + title + '.txt'
            if os.path.exists(save_file):
                print('file %s is already exist!' % title)
                return None
            with open(save_file, "w") as file_input:
                file_input.write(summary)
        except Exception as e:
            print('save file error.'+str(e))
        return crawl_result

    def save(self, crawl_result, process_pool):
        process_pool.submit(self._output_runnable, crawl_result)


class CrawlManager(object):
    '''
    爬虫管理类，进程池负责统一管理调度爬取解析及存储进程
    '''
    def __init__(self):
        self.crawl = CrawlProcess()
        self.output = OutPutProcess()
        self.crawl_pool = ProcessPoolExecutor(max_workers=8)
        self.crawl_deep = 100   #爬取深度
        self.crawl_cur_count = 0

    def _crawl_future_callback(self, crawl_url_future):
        try:
            data = crawl_url_future.result()
            self.output.save(data, self.crawl_pool)
            for new_url in data['new_urls']:
                self.start_runner(new_url)
        except Exception as e:
            print('Run crawl url future process error. '+str(e))

    def start_runner(self, url):
        if self.crawl_cur_count > self.crawl_deep:
            return
        self.crawl_cur_count += 1
        self.crawl.crawl(url, self._crawl_future_callback, self.crawl_pool)
 """

if __name__ == '__main__':
    d = Dispatcher()
    d.start()