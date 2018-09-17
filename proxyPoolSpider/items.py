# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ProxypoolspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    ip = scrapy.Field()
    port = scrapy.Field()
    address = scrapy.Field()
    anonymous = scrapy.Field()
    http = scrapy.Field()
    resptime = scrapy.Field()
    alivetime = scrapy.Field()
    testtime = scrapy.Field()
    domain = scrapy.Field()
    kinddesc = scrapy.Field()
    # 是否验证 0-否； 1-是
    status = scrapy.Field()
    # reslist
    reslist = scrapy.Field()
    url = scrapy.Field()
