import datetime
import json
import random
import time
import requests
import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

cookie = """ """

def get_data(url,cat_cnt,audience_id):

    features = ["career", "gender", "ecom_big8_audience", "job", "life_stage", "phone_price_preference", "province","city","cityLevel","yuntu_city_level","age","consuming_capacity","phone_brand","douyin_active_user"]
    url_args = {
    "app": "",
    "audience_cnt": cat_cnt,
    "audience_id": audience_id,
    "features": features
    }
    
    headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7,zh-TW;q=0.6",
    "content-length": "272",
    "content-type": "application/json;charset=UTF-8",
    "cookie": cookie,
    "origin": "https://yuntu.oceanengine.com",
    "referer": "",
    "sec-ch-ua-mobile": "?0",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
    }
    
    s = json.dumps(url_args)
    r =requests.post(url=url,headers=headers,data=s)
    return r.text


def data_process(data,filname):
    json_data = json.loads(data)
    data = json_data['data']['data']
    portrait_dict = {}
    for i in range(len(data)):
        name_zh = data[i]['name_zh']
        label_list = data[i]['label_list']
        tag_dict = {}
        for j in range(len(label_list)):
            tag_name = label_list[j]['name_zh']
            value = label_list[j]['value']
            #tgi = label_list[j]['tgi']
            tag_dict[tag_name] = value
        portrait_dict[name_zh] = tag_dict
    
    columns = ["预测性别","预测年龄段","城市级别","电商八大人群","预测消费能力"]
    filname = filname + ".xlsx"
    writer = pd.ExcelWriter(filname)
    
    for i in range(len(columns)):
        df =  pd.DataFrame.from_dict(portrait_dict[columns[i]],orient='index',columns=['占比'])
        df = df.reset_index().rename(columns={'index':'标签'})
        df.to_excel(writer,sheet_name=columns[i],index=False)
    writer.save()
    
    
#-----------------爬取入口--------------

portrait_list = pd.read_excel('人群列表.xlsx')

# 参数选择
cat_cnts = portrait_list['data__audiences__cover_num_by_app_aweme'].tolist()
audience_ids = portrait_list['data__audiences__id'].tolist()
names = portrait_list['data__audiences__name'].tolist()

# 开始爬取
url = "https://yuntu.oceanengine.com/yuntu_ng/api/v1/get_customeaudience_portrait?aadvid="
count = 0
for cat_cnt,audience_id,filname in zip(cat_cnts,audience_ids,names):
    data = get_data(url,cat_cnt,audience_id)
    data_process(data,filname)
    count += 1
    print(count,filname)
    time.sleep(random.uniform(0.5,1.5))
    
