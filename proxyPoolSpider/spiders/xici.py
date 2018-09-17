# -*- coding: utf-8 -*-
import scrapy
from bs4 import BeautifulSoup
import traceback
from proxyPoolSpider.dbutils import DbUtils
from proxyPoolSpider.items import ProxypoolspiderItem
import  random

DESC = {'nn': '国内高匿', 'nt': '国内普通', 'wn': '国外高匿', 'wt': '国外普通'}

PAGENUM = range(1, 4)

'''
    www.xicidaili.com 西刺免费代理抓取  
    1. 抓取的代理若在库中已经存在，则不重复插入 
    2. 后台需要单独启动脚本来进行验证 ，对验证不通过的进行删除操作， 通过的放到新库中
'''
class XiciSpider(scrapy.Spider):
    name = 'xici'
    allowed_domains = ['www.xicidaili.com']
    dbUtil = DbUtils()
    # start_urls = ['http://www.xicidaili.com/nn/']

    def start_requests(self):
        for kind in DESC:
            for p in PAGENUM:
                print("开始处理page：", p, ' 代理类别：', kind, DESC.get(kind))
                url = 'http://www.xicidaili.com/' + kind + '/' + str(p)
                print("URL=", url)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    meta={
                        'kind': kind,
                        'kinddesc': DESC.get(kind),
                        'page': p,
                        'domain': 'www.xicidaili.com'
                    })
            

    def parse(self, response):
        kind = response.meta['kind']
        kinddesc = response.meta['kinddesc']
        page = response.meta['page']
        domain = response.meta['domain']

        reslist = []
        text = response.text

        print("解析响应：", response.status)
        print(len(text))

        #f = open('response.txt', 'w')
        #f.write(text)

        try:
            soup = BeautifulSoup(text, 'html.parser')
            # print(soup)
            # #ip_list > tbody > tr:nth-child(2)
            # #ip_list > tbody > tr:nth-child(2) > td:nth-child(7) > div
            # trs = soup.select("table > tbody > tr")
            tt = soup.select("#ip_list")
            # print(tt)
            trs = tt[0].find_all("tr")

            for tr in trs[1:]:
                tds = tr.find_all('td')

                #print('===============')
                #print(len(tds))
                # print(tds)
                res = {}

                # 1.国家 2.ip 3.port 4.地址 5.niming 6.类型 7.速度 8.响应时间 9cunhuo 10.验证时间
                res.update({'ip': tds[1].getText()})
                res.update({'port': tds[2].getText()})
                res.update({'address': tds[3].getText().strip()})
                res.update({'anonymous': self.toAnoymousType(tds[4].getText())})
                res.update({'http': self.tohttp(tds[5].getText())})
                res.update({'speed': tds[6].find('div')['title']})
                res.update({'resptime': tds[7].find('div')['title']})
                res.update({'alivetime': tds[8].getText()})
                res.update({'testtime': tds[9].getText()})
                res.update({'portimage': ''}) # 用于保存验证码图像 

                reslist.append(res)

        except Exception as e:
            traceback.print_exc()
            excstr = traceback.format_exc()
            self.dbUtil.saveCrawLog(response.url, kinddesc, 'error',
                                    'response解析异常, ' + excstr)
            raise e

        print(response.url, "解析结果数据为：", len(reslist))

        item = ProxypoolspiderItem()
        item['reslist'] = reslist
        item['domain'] = domain
        item['kinddesc'] = kinddesc
        item['url'] = response.url

        yield item

        print("解析入库完成")

# 匿名类型
# 高匿  、 高匿名 、 普匿 、 透明 、 透明代理IP、 普通代理ip

    def toAnoymousType(self, str):
        if str.find('高匿') > -1 or str.find('普匿') > -1:
            return 'A'
        elif str.find('普通') > -1:
            return 'C'
        elif str.find('透明') > -1:
            return 'T'
        return 'N'

### 统一将 http  https 转换为大写形式

    def tohttp(self, str):
        return str.upper()

    ## 查询数据中的取当前最新的代理地址  
    def getrandomproxy(self):

        index = random.randint(0,100) % 30
        res = self.dbUtil.findProxypool(30)
        return res[index].get('purl')

#  for test
def main():
    htmlstr = '''
	'''


if __name__ == '__main__':
    main()
