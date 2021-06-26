import requests
import pandas as pd
import json
from bs4 import BeautifulSoup

provinceName = requests.get('https://api.inews.qq.com/newsqa/v1/query/inner/publish/modules/list?modules=chinaDayList,chinaDayAddList,nowConfirmStatis,provinceCompare')
provinceNameList = list(provinceName.json()['data']['provinceCompare'].keys())

#https://news.qq.com/zt2020/page/feiyan.htm#/
#腾讯新闻
urlRoot = 'https://api.inews.qq.com/newsqa/v1/query/pubished/daily/list?province='

for p in provinceNameList:
    url = urlRoot + p
    history = requests.get(url).json()
    history_data = pd.DataFrame(history['data'])
    history_data.to_csv("./tencent/" + p +".csv",sep=',',index=False,header=True,encoding='utf_8_sig')
