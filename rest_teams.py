import scrapy
from scrapy.crawler import CrawlerProcess
from selenium import webdriver
from datetime import datetime
from datetime import date
from scrapy import exporters
import json

from nba_profit.items import AccountItem
from nba_profit.items import TweetItem


driver = webdriver.Chrome("D:/Software/chromedriver.exe")
user_agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0'
referer = 'https://twitter.com/search-home'

# set the time and scrape backwards until this time
# last few tweets might be a little bit older than the time, but all tweets afterwards are guaranteed to be recorded
start_date = datetime.timestamp(datetime(2019, 1, 1, 0, 0, 0))


class RestAccountSpider(scrapy.Spider):
    name = 'rest_team_official_account_spider'
    custom_settings = {'DOWNLOAD_DELAY': 3}

    # #### method to type in team names and search the verified account
    def start_requests(self):
        urls = ['https://twitter.com/nyknicks', 'https://twitter.com/LAClippers', 'https://twitter.com/trailblazers',
                'https://twitter.com/okcthunder', 'https://twitter.com/Pacers', 'https://twitter.com/MiamiHEAT']

        urls = ['https://twitter.com/Timberwolves', 'https://twitter.com/BrooklynNets']

        # loop through all the teams with the url
        for i in range(len(urls)):
            url = urls[i]
            yield scrapy.Request(url=url, headers={'User-Agent': user_agent, 'Referer': referer},
                                 callback=self.parse_account)

    def parse_account(self, response):
        driver.get(response.request.url)
        sel = scrapy.Selector(text=driver.page_source)

        item = AccountItem()
        item['name'] = sel.xpath('//h1[@class="ProfileHeaderCard-name"]/a/text()').extract_first()
        item['link'] = response.request.url
        item['tweets_cnt'] = sel.xpath('//a[@data-nav="tweets"]//span[@class="ProfileNav-value"]/@data-count'
                                       ).extract_first()
        item['followers_cnt'] = sel.xpath('//a[@data-nav="followers"]//span[@class="ProfileNav-value"]/@data-count')\
            .extract_first()
        self.writing_account(item)
        yield AccountItem

        # ################### First, scrape the most updated tweet from current page
        tweet_item = TweetItem()
        first_tweet = sel.xpath('//li[@data-item-type="tweet"][1]')
        tweet_time = first_tweet.xpath('//span[contains(@class,"_timestamp")]/@data-time').extract_first()
        tweet_item['time'] = datetime.utcfromtimestamp(int(tweet_time)).strftime('%Y-%m-%d %H:%M:%S')
        tweet_item['id'] = first_tweet.xpath('@data-item-id').extract_first()
        tweet_item['link'] = item['link'] + '/status/' + first_tweet.xpath('@data-item-id').extract_first()
        tweet_item['tweeter'] = first_tweet.xpath('//strong[contains(@class, "fullname show-popup-with-id")]/text()'
                                                  ).extract_first()

        # tweet content in txt needs to be cleaned
        t_txt = first_tweet.xpath('//div[@class="js-tweet-text-container"]/p//text()').extract()
        tweet_item['txt'] = ''
        for j in range(len(t_txt)):
            tweet_item['txt'] += t_txt[j]
        tweet_item['txt'] = tweet_item['txt'].strip().replace('\n', ' ')

        # counts of comments/retweets/likes could be none, use 0 to fill in
        c_cnt = first_tweet.xpath('//button[@data-modal="ProfileTweet-reply"]//span[@class='
                                  '"ProfileTweet-actionCountForPresentation"]/text()').extract_first()
        r_cnt = first_tweet.xpath('//button[@data-modal="ProfileTweet-retweet"]//span[@class='
                                  '"ProfileTweet-actionCountForPresentation"]/text()').extract_first()
        l_cnt = first_tweet.xpath('//button[@class="ProfileTweet-actionButton js-actionButton js-actionFavorite"]'
                                  '//span[@class="ProfileTweet-actionCountForPresentation"]/text()').extract_first()
        tweet_item['comment_cnt'] = (0 if c_cnt is None else c_cnt)
        tweet_item['retweet_cnt'] = (0 if r_cnt is None else r_cnt)
        tweet_item['liked_cnt'] = (0 if l_cnt is None else l_cnt)
        yield TweetItem


        # ########### Second, load more tweets
        # twitter automatically loads 20 more tweets when scrolled down to bottom chronologically.
        # mechanism is to visit following link to the json file. Link uses max_position=last tweet's ID to load 20 more.
        # https://twitter.com/i/profiles/show/xxxxxx/timeline/tweets?include_available_features=1&include_entities=1&max_position=xxxxx&reset_error_state=false

        # construct the common form for the json url and fill in the first tweet's id to load 20 more
        link = item['link'].replace('twitter.com', 'twitter.com/i/profiles/show') + \
               '/timeline/tweets?include_available_features=1&include_entities=1&max_position={0}' \
               '&reset_error_state=false'
        new_link = link.format(tweet_item['id'])
        scroll = scrapy.Request(url=new_link, headers={'User-Agent': user_agent, 'Referer': item['link']},
                                callback=self.load_more)
        # item needs to be passed to get the team name and account homepage, which will be used to record tweet info
        scroll.meta['item'] = item
        yield scroll

    # method to keep loading 20 more tweets and parse each tweet's information, until the oldest time wanted is met
    def load_more(self, response):
        # inherit the account item
        item = response.meta['item']

        # loop what's been done to the most updated tweet to parse each tweet loaded
        data = json.loads(response.body)['items_html']
        count = json.loads(response.body)['new_latent_count']
        sel = scrapy.Selector(text=data)
        for i in range(count):
            tweet_path = '//li[@data-item-type="tweet"][{0}]'.format(i+1)
            tweet_item = TweetItem()
            t_id = sel.xpath(tweet_path+'/@data-item-id').extract_first()
            tweet_item['id'] = t_id
            tweet_item['link'] = item['link'] + '/status/' + t_id
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
            self.writing_tweets(tweet_item, item['name'])

        # check if the last loaded tweet's time has already met the set time.
        # If not, keep loading 20 more. New link just needs to renew the tweet id part
        if int(t_time) > start_date and json.loads(response.body)['has_more_items']:
            old_link = response.request.url
            new_link = old_link.replace(old_link[old_link.find("max_position")+len("max_position="):
                                                 old_link.find("&reset_error")], t_id)
            continue_scroll = scrapy.Request(url=new_link,
                                             headers={'User-Agent': user_agent, 'Referer': item['link']},
                                             callback=self.load_more)
            continue_scroll.meta['item'] = item
            yield continue_scroll
        else:
            print('finished with team:!')
            print(item['name'])

    # method to write account statistics. file name marks the date that the code is run because counts could increase.
    def writing_account(self, item):
        filename = "account stats (" + date.today().strftime('%d%m') + ').csv'
        file = open(filename, "ab")
        export = exporters.CsvItemExporter(file, encoding='utf-8', include_headers_line=False)
        export.export_item(item)

    # method to write account statistics. file name marks the date that the code is run because counts could increase.
    def writing_tweets(self, item, team_name):
        filename = "officialAccountsTweets/" + team_name + ' Tweets ('\
                   + date.today().strftime('%d%m') + ').csv'
        file = open(filename, "ab")
        export = exporters.CsvItemExporter(file, encoding='utf-8', include_headers_line=False)
        export.export_item(item)


process = CrawlerProcess()
process.crawl(RestAccountSpider)
process.start()
