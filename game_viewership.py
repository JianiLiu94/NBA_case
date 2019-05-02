# data source:
# https://matchweb.sports.qq.com/kbs/list?from=NBA_PC&columnId=100000&startTime=2018-10-13&endTime=2019-04-11&from=sporthp&callback=ajaxExec&_=1556654135895
# referer: https://nba.stats.qq.com/schedule/

import scrapy
from scrapy.crawler import CrawlerProcess
from selenium import webdriver

import json
import pandas as pd
import statistics
import csv

driver = webdriver.Chrome("D:/Software/chromedriver.exe")
user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'

raw_data = open("viewership_tencent.txt", "r").readline()
data = json.loads(raw_data)
games = pd.DataFrame()

for key, value in data.items():
    new_games = pd.DataFrame(value)
    new_games['date'] = key
    games = games.append(new_games, sort='date')
games = games[games['matchType'] == '2']
# games.to_csv("schedule_tencent.csv")

output_file = open("viewership_data.csv", "w+", newline='')
writer = csv.writer(output_file)


class Viewership(scrapy.Spider):
    name = 'viewership_scrape'
    # custom_settings = {'DOWNLOAD_DELAY': 3}

    def start_requests(self):
        url_default = "https://kbs.sports.qq.com/kbsweb/game.htm?mid={0}&replay=1"
        for mid in games["mid"][800:]:
            url = url_default.format(mid)
        # urls = ["https://kbs.sports.qq.com/kbsweb/game.htm?mid=100000:5990&replay=1",
        #         'https://kbs.sports.qq.com/kbsweb/game.htm?mid=100000:6996&replay=1']
        # for url in urls:
        #     # url = "https://kbs.sports.qq.com/kbsweb/game.htm?mid=100000:5990&replay=1"
            yield scrapy.Request(url=url,
                                 headers={'User-Agent': user_agent, 'Referer': 'https://nba.stats.qq.com/schedule/'},
                                 callback=self.parse
                                 )

    def parse(self, response):
        url = response.request.url
        driver.get(url)

        sel = scrapy.Selector(text=driver.page_source)
        mid = url.replace('https://kbs.sports.qq.com/kbsweb/game.htm?mid=', '').replace('&replay=1', '')
        viewers = []

        for item in sel.xpath('//div[contains(@class,"video-item")]'):
            if item.xpath('.//div[@class="tag"]/text()').extract_first() == "回放":
                viewer = item.xpath('.//span[@class="views"]/text()').extract_first()
                if "万" in viewer:
                    viewer = int(float(viewer.replace('播放：', '').replace('万', ''))*10000)
                else:
                    viewer = int(viewer.replace('播放：', ''))
                viewers.append(viewer)

        max_viewer = max(viewers)
        min_viewer = min(viewers)
        avg_viewer = statistics.mean(viewers)
        row = [mid, max_viewer, min_viewer, avg_viewer]
        print(row)
        writer.writerow(row)


process = CrawlerProcess()
process.crawl(Viewership)
process.start()
