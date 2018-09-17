import scrapy
from bs4 import BeautifulSoup
import traceback
from proxyPoolSpider.dbutils import DbUtils
from proxyPoolSpider.items import ProxypoolspiderItem
import  random

DESC = {'1': '国内高匿', '2': '国内普通', '3': '国外高匿', '4': '国外普通'}

PAGENUM = range(1, 7)


'''
    云代理抓取 ：  http://www.ip3366.net/free/
    url拼接： http://www.ip3366.net/free/?stype=kind&page=1 

    匿名状态如下：  
    
'''
class cloudSpider(scrapy.Spider):
    name = 'cloud'
    allowed_domains = ['www.ip3366.net']
    dbUtil = DbUtils()
    def start_requests(self):
        for kind in DESC:
            for p in PAGENUM:
                print("开始处理page：", p, ' 代理类别：', kind, DESC.get(kind))
                url = 'http://www.ip3366.net/free/?stype=' + kind + '&page=' + str(p)
                print("URL=", url)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    meta={
                        'kind': kind,
                        'kinddesc': DESC.get(kind),
                        'page': p,
                        'domain': 'www.ip3366.net'
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

        try:
            soup = BeautifulSoup(text, 'html.parser')
            # print(soup)
            # #ip_list > tbody > tr:nth-child(2)
            # #ip_list > tbody > tr:nth-child(2) > td:nth-child(7) > div
            # trs = soup.select("table > tbody > tr")
            tt = soup.select("#list")
            # print(tt)
            trs = tt[0].find_all("tr")

            for tr in trs[1:]:
                tds = tr.find_all('td')

                #print('===============')
                #print(len(tds))
                # print(tds)
                res = {}

                # 1.ip 2.port 3.niming  4.类型  5.位置 6.响应时间 7.验证时间
                res.update({'ip': tds[0].getText()})
                res.update({'port': tds[1].getText()})
                res.update({'address': tds[4].getText().strip()})
                ## 增加国外的判断
                res.update({'anonymous': self.toAnoymousType(tds[2].getText(), kind)})
                res.update({'http': self.tohttp(tds[3].getText())})
                res.update({'speed': tds[5].getText()})
                res.update({'resptime': tds[6].getText()})
                res.update({'alivetime': ''})
                res.update({'testtime': ''})
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
    def toAnoymousType(self, str, kind):

        if kind == "1" or kind == "2": 
            if str.find('高匿') > -1 or str.find('普匿') > -1:
                return 'A'
            elif str.find('普通') > -1:
                return 'C'
            elif str.find('透明') > -1:
                return 'T'
            return 'N'
        else:
            if str.find('高匿') > -1 or str.find('普匿') > -1:
                return 'WA'
            elif str.find('普通') > -1:
                return 'WC'
            elif str.find('透明') > -1:
                return 'WT'
            return 'WN'

    ### 统一将 http  https 转换为大写形式
    def tohttp(self, str):
        return str.upper()