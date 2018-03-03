import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Spider
from scrapy.crawler import CrawlerProcess
#from scrapy import optional_features
#optional_features.remove('boto')
import lxml
import sqlite3

import iditarod as idt

import datetime

engine = idt.loadEngine(idt.DB_LOCATION)
session = idt.loadSession(engine)
class Iditarod(Spider):
    name = "iditarod"
    #DATABASE_LOCATION = "../data/web_pages.db"
    years = [i+ 2010 for i in range(8)]
    def start_requests(s):
      '''
      called as the first request.  only runs once
      used to build database connection
      '''
      urls = ["http://iditarod.com/race/%s/mushers/list/"%x for x in s.years]
      return list([scrapy.Request(x , s.get_musher) for x in urls])


    def get_musher(s, response):
      '''
      writes the things to the places!
      also extracts urls and continues to scrapeeee away
      '''

      mushers = response.xpath("//table[@class='stats-table']/tr/td/a/@href").extract()
      
      for musher in mushers:
        yield scrapy.Request(str(musher), callback=s.scrape_musher)
    
    def scrape_musher(s, response):
      racer =' '.join(str(response.url).split('/')[-2].split('-')[1:])
      race = str(response.url).split('/race/')[1][:4]
      checkpoints = response.xpath("//div[@class='stats-table-wrapper']/table/tbody/tr")
      for checkpoint in checkpoints:
        name = checkpoint.xpath("td[1]/a/text()").extract()[0]
        time = checkpoint.xpath("td[2]/text()").extract()
        if time != []:
          time = datetime.datetime.strptime(
              race+"/"+time[0], 
              "%Y/%m/%d %H:%M:%S"
              )
          session.add(idt.CheckpointCrossing(
            racer=racer,
            race=race,
            checkpoint=name,
            timestamp=time
            ))
          session.commit()
          print('ADDED')
process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(Iditarod, allowed_domains = ["www.iditarod.com", "iditarod.com"])
process.start() # the script will block here until the crawling is finished



  
  
  
