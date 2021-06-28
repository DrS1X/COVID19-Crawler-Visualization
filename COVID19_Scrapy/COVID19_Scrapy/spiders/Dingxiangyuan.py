import scrapy
import json
from bs4 import BeautifulSoup
from COVID19_Scrapy.items import Covid19ScrapyItem
import time

class DingxiangyuanSpider(scrapy.Spider):
    name = 'Dingxiangyuan'
    allowed_domains = ['ncov.dxy.cn','file1.dxycdn.com']
    start_urls = ['https://ncov.dxy.cn/ncovh5/view/pneumonia']   
      
    def parse(self, response):
        soup=BeautifulSoup(response.text,'html.parser')    
        htmlBodyText=soup.body.text
        provinceDataText = htmlBodyText[htmlBodyText.find('window.getAreaStat = '):]
        provinceDataStr = provinceDataText[provinceDataText.find('[{'):provinceDataText.find('}catch')]
        provinceDataJson=json.loads(provinceDataStr)
#        testList = []
#        testList.append(provinceDataJson[0])
        for p in provinceDataJson:
            province_url = p['statisticsData']
#            item = 
            yield scrapy.Request(province_url,
                                 callback=self.parseProvince,
                                 cb_kwargs=dict(provinceName=p['provinceName']))
        
    def parseProvince(self, response, provinceName):
#        response.encoding = 'utf-8'
        history = json.loads(response.text)
        item = Covid19ScrapyItem()
        item['resource'] = 'Dingxiangyuan'
        item['province'] = provinceName
        item['updateDate'] = time.strftime("%Y-%m-%d", time.localtime())
        item['data'] = history['data']           
        return item
