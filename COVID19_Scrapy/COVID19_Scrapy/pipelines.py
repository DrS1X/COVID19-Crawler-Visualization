# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd
import os

class Covid19ScrapyPipeline:
    def process_item(self, item, spider):        
#        pwd = os.getcwd() 
        folder = './'+ item['resource'] +'_UpdateAt'+ item['updateDate']
        if not os.path.exists(folder):               
            os.makedirs(folder)                 

        history_data = pd.DataFrame(item['data'])
        history_data.to_csv(folder + '/' + item['province'] + ".csv",sep=',',index=False,header=True)
