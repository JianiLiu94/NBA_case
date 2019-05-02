import scrapy
from scrapy.crawler import CrawlerProcess
from selenium import webdriver
from datetime import datetime
import pandas as pd
from datetime import date
from scrapy import exporters
import json

from nba_profit.items import TweetItem


driver = webdriver.Chrome("D:/Software/chromedriver.exe")
user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'

# set the time and scrape backwards until this time
# last few tweets might be a little bit older than the time, but all tweets afterwards are guaranteed to be recorded
start_date = datetime.timestamp(datetime(2018, 10, 13, 0, 0, 0))
end_date = datetime.timestamp(datetime(2019, 4, 13, 0, 0, 0))
start = datetime.utcfromtimestamp(int(start_date)).strftime('%Y-%m-%d')
end = datetime.utcfromtimestamp(int(end_date)).strftime('%Y-%m-%d')

url = 'https://twitter.com/search?f=tweets&vertical=default&q=from%3A{0}%20since%3A{1}%20until%3A{2}&src=typd'
json_url = 'https://twitter.com/i/search/timeline?f=tweets&vertical=default&q=from%3A{0}%20since%3A{1}%20until%3A{2}&' \
           'src=typd&include_available_features=1&include_entities=1&max_position={3}&reset_error_state=false'


class TimePeriodSpider(scrapy.Spider):
    name = 'within_time_period_spider'
    custom_settings = {'DOWNLOAD_DELAY': 3}

    # #### method to type in team names and search the verified account
    def start_requests(self):
        accounts = pd.read_csv('account_info.csv', header=None)
        accounts.columns = ['account', 'team']

        # loop through all the teams with the url
        for account in accounts['account']:
            url_search = url.format(account, start, end)
            yield scrapy.Request(url=url_search,
                                 headers={'User-Agent': user_agent, 'Referer': 'https://twitter.com/search-home'},
                                 callback=self.parse_search)

    def parse_search(self, response):
        driver.get(response.request.url)
        sel = scrapy.Selector(text=driver.page_source)
        index_1 = response.request.url.find("=from%3A") + len("=from%3A")
        index_2 = response.request.url.find("%20since%3A")
        account = response.request.url[index_1:index_2]

        for i in range(len(sel.xpath('//li[@data-item-type="tweet"]'))):
            tweet_item = TweetItem()
            tweet_path = '//li[@data-item-type="tweet"][{0}]'.format(i+1)
            tweet_time = sel.xpath(tweet_path+'//span[contains(@class,"_timestamp")]/@data-time').extract_first()
            tweet_item['time'] = datetime.utcfromtimestamp(int(tweet_time)).strftime('%Y-%m-%d %H:%M:%S')
            tweet_item['id'] = sel.xpath(tweet_path+'/@data-item-id').extract_first()
            tweet_item['link'] = 'http://twitter.com/' + account + '/status/' + tweet_item['id']
            tweet_item['tweeter'] = sel.xpath(tweet_path+'//strong[contains(@class, "fullname show-popup-with-id")]/text()'
                                                      ).extract_first()

            # tweet content in txt needs to be cleaned
            # test = sel.xpath(tweet_path+'//div[@class="js-tweet-text-container"]/p//text()').extract()
            t_txt = sel.xpath(tweet_path+'//div[@class="js-tweet-text-container"]/p//text()').extract()
            tweet_item['txt'] = ''
            for j in range(len(t_txt)):
                tweet_item['txt'] += t_txt[j]
            tweet_item['txt'] = tweet_item['txt'].strip().replace('\n', ' ')

            # counts of comments/retweets/likes could be none, use 0 to fill in
            c_cnt = sel.xpath(tweet_path+'//button[@data-modal="ProfileTweet-reply"]//span[@class='
                                      '"ProfileTweet-actionCountForPresentation"]/text()').extract_first()
            r_cnt = sel.xpath(tweet_path+'//button[@data-modal="ProfileTweet-retweet"]//span[@class='
                                      '"ProfileTweet-actionCountForPresentation"]/text()').extract_first()
            l_cnt = sel.xpath(tweet_path+'//button[@class="ProfileTweet-actionButton js-actionButton js-actionFavorite"]'
                                      '//span[@class="ProfileTweet-actionCountForPresentation"]/text()').extract_first()
            tweet_item['comment_cnt'] = (0 if c_cnt is None else c_cnt)
            tweet_item['retweet_cnt'] = (0 if r_cnt is None else r_cnt)
            tweet_item['liked_cnt'] = (0 if l_cnt is None else l_cnt)
            self.writing_tweets(tweet_item, account)
            yield TweetItem

        max_position = sel.xpath('//div[@class="stream-container"]/@data-max-position').extract_first()
        scroll_url = json_url.format(account, start, end, max_position)
        yield scrapy.Request(url=scroll_url,
                             headers={'User-Agent': user_agent, 'Referer': response.request.url},
                             callback=self.load_more)

    def load_more(self, response):
        index_1 = response.request.url.find("=from%3A") + len("=from%3A")
        index_2 = response.request.url.find("%20since%3A")
        account = response.request.url[index_1:index_2]

        data = json.loads(response.body)['items_html']
        count = json.loads(response.body)['new_latent_count']
        max_position = json.loads(response.body)['min_position']
        sel = scrapy.Selector(text=data)
        for i in range(count):
            tweet_path = '//li[@data-item-type="tweet"][{0}]'.format(i+1)
            tweet_item = TweetItem()
            t_id = sel.xpath(tweet_path+'/@data-item-id').extract_first()
            tweet_item['id'] = t_id
            tweet_item['link'] = 'http://twitter.com/' + account + '/status/' + t_id
            t_time = sel.xpath(tweet_path+'//span[contains(@class,"_timestamp")]/@data-time').extract_first()
            tweet_item['time'] = datetime.utcfromtimestamp(int(t_time)).strftime('%Y-%m-%d %H:%M:%S')
            tweet_item['tweeter'] = sel.xpath(tweet_path+'//strong[contains(@class, "fullname show-popup-with-id")'
                                                         ']/text()').extract_first()
            t_txt = sel.xpath(tweet_path+'//div[@class="js-tweet-text-container"]/p//text()').extract()
            tweet_item['txt'] = ''
            for j in range(len(t_txt)):
                tweet_item['txt'] += t_txt[j]
            tweet_item['txt'] = tweet_item['txt'].strip().replace('\n', ' ')
            c_cnt = sel.xpath(tweet_path+'//button[@data-modal="ProfileTweet-reply"'
                                         ']//span[@class="ProfileTweet-actionCountForPresentation"]/text()'
                              ).extract_first()
            r_cnt = sel.xpath(tweet_path + '//button[@data-modal="ProfileTweet-retweet"]//span[@class="'
                                           'ProfileTweet-actionCountForPresentation"]/text()').extract_first()
            l_cnt = sel.xpath(tweet_path+'//button[@class="ProfileTweet-actionButton js-actionButton js-'
                                         'actionFavorite"]//span[@class="ProfileTweet-actionCountForPresentation"'
                                         ']/text()').extract_first()
            tweet_item['comment_cnt'] = (0 if c_cnt is None else c_cnt)
            tweet_item['retweet_cnt'] = (0 if r_cnt is None else r_cnt)
            tweet_item['liked_cnt'] = (0 if l_cnt is None else l_cnt)
            yield TweetItem
            self.writing_tweets(tweet_item, account)

        if json.loads(response.body)['has_more_items']:
            new_link = json_url.format(account, start, end, max_position)
            yield scrapy.Request(url=new_link,
                                 headers={'User-Agent': user_agent, 'Referer': url.format(account, start, end)},
                                 callback=self.load_more)
        else:
            print('finished with team:!')
            print(account)

    # method to write account statistics. file name marks the date that the code is run because counts could increase.
    def writing_tweets(self, item, team_name):
        filename = "RegularSeasonTweets/" + team_name + ' Tweets ('\
                   + date.today().strftime('%d%m') + ').csv'
        file = open(filename, "ab")
        export = exporters.CsvItemExporter(file, encoding='utf-8', include_headers_line=False)
        export.export_item(item)


process = CrawlerProcess()
process.crawl(TimePeriodSpider)
process.start()
