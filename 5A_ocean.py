import datetime
import json
import random
import time
import requests
import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

def data_process(date,level1_list):
    detail_list = []
    for i in range(len(level1_list)):
        #触点分布
        name_zh = level1_list[i]["name_zh"]
        
        #本品牌
        value = level1_list[i]["value"]
        #对比品牌均值
        competitor_value = level1_list[i]["competitor_value"]
        #行业TOP5均值
        value_industry_5 = level1_list[i]["value_industry_5"]
        #行业TOP20均值
        value_industry_20 = level1_list[i]["value_industry_20"]
        #行业TOP50均值
        value_industry_50 = level1_list[i]["value_industry_50"]
        #行业TOP100均值
        value_industry_100 = level1_list[i]["value_industry_100"]
        
        #本品牌（占比）
        percent = level1_list[i]["percent"]
        #对比品牌均值（占比）
        competitor_percent = level1_list[i]["competitor_percent"]
        #行业TOP5均值（占比）
        percent_industry_5 = level1_list[i]["percent_industry_5"]
        #行业TOP20均值（占比）
        percent_industry_20 = level1_list[i]["percent_industry_20"]
        #行业TOP50均值（占比）
        percent_industry_50 = level1_list[i]["percent_industry_50"]
        #行业TOP100均值（占比）
        percent_industry_100 = level1_list[i]["percent_industry_100"]

        detail_list.append((date,name_zh,value,competitor_value,value_industry_5,value_industry_20,value_industry_50,value_industry_100,percent,competitor_percent,percent_industry_5,percent_industry_20,percent_industry_50,percent_industry_100))

        for j in range(len(level1_list[i]["children"])):
            name = name_zh + "_" +  level1_list[i]["children"][j]["name"]
            
            value =  level1_list[i]["children"][j]["value"]
            competitor_value =  level1_list[i]["children"][j]["competitor_value"]
            value_industry_5 =  level1_list[i]["children"][j]["value_industry_5"]
            value_industry_20 =  level1_list[i]["children"][j]["value_industry_20"]
            value_industry_50 =  level1_list[i]["children"][j]["value_industry_50"]
            value_industry_100 =  level1_list[i]["children"][j]["value_industry_100"]
            
            percent =  level1_list[i]["children"][j]["percent"]
            competitor_percent =  level1_list[i]["children"][j]["competitor_percent"]
            percent_industry_5 =  level1_list[i]["children"][j]["percent_industry_5"]
            percent_industry_20 =  level1_list[i]["children"][j]["percent_industry_20"]
            percent_industry_50 =  level1_list[i]["children"][j]["percent_industry_50"]
            percent_industry_100 =  level1_list[i]["children"][j]["percent_industry_100"]

            detail_list.append((date,name,value,competitor_value,value_industry_5,value_industry_20,value_industry_50,value_industry_100,percent,competitor_percent,percent_industry_5,percent_industry_20,percent_industry_50,percent_industry_100))

    return detail_list
    
def getDay(date_str,num): 
    # today=datetime.date.today() 
    today = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    oneday=datetime.timedelta(days=num) 
    yesterday=today+oneday  
    yesterday1 = yesterday.strftime("%Y%m%d")
    yesterday2 = yesterday.strftime("%Y-%m-%d")
    return (yesterday1,yesterday2)

def get_content(url,cookie):
    headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh;q=0.9",
    "cache-control": "no-cache",
    "cookie": cookie,
    "pragma": "no-cache",
    "referer": "",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"
    
    }
    response=requests.get(url=url,headers=headers)
    data = json.loads(response.text)
    return data
    
#-----------------爬取入口--------------
cookie = """ """

# 参数选择
args = {
    "人群总资产": 0,
    "了解(Aware)": 1,
    "吸引(Appeal)": 2,
    "问询(Ask)": 3,
    "行动(Act)": 4,
    "拥护(Advocate)": 5
}

base_url = "https://yuntu.oceanengine.com/yuntu_ng/api/v1/get_audience_asset_trigger_point_distribution?"
day_num = 47
date_pick = "2021-05-01"
urls = []
cat_pick = "了解(Aware)"
count = 0
columns = ["日期","触点分布","本品牌","对比品牌均值","行业TOP5均值","行业TOP20均值","行业TOP50均值","行业TOP100均值","本品牌（占比）","对比品牌均值（占比）","行业TOP5均值（占比）","行业TOP20均值（占比）","行业TOP50均值（占比）","行业TOP100均值（占比）"]

for i in range(day_num+1):
    dates = getDay(date_pick,i)
    url_args = {
    'aadvid': '',
     'industry_id': '',
     'brand_id': '',
     'date':  str(dates[0]),
     'card': args[cat_pick]
    }
    new_query = urlencode(url_args, doseq=True)
    url = base_url + new_query
    
    #开始爬取
    data = get_content(url,cookie)
    dy = data['data']["trigger_point_distribution"][0]
    level1_list = dy["children"]    
    detail_list = data_process(dates[1],level1_list)
    time.sleep(random.uniform(0.5,1.5))
    count += 1
    print(count,url)
    filname = "PD巨量云图_" + cat_pick + "_触点分布_" + dates[1] + ".csv"
    details=pd.DataFrame(columns=columns,data=detail_list)
    print(details.info())
    #解决中文乱码
    details.to_csv(filname,encoding = "utf_8_sig")

