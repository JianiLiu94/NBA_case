import scrapy


class AccountItem(scrapy.Item):

    name = scrapy.Field()
    link = scrapy.Field()
    tweets_cnt = scrapy.Field()
    followers_cnt = scrapy.Field()


class TweetItem(scrapy.Item):

    id = scrapy.Field()
    link = scrapy.Field()
    time = scrapy.Field()
    tweeter = scrapy.Field()
    txt = scrapy.Field()
    comment_cnt = scrapy.Field()
    retweet_cnt = scrapy.Field()
    liked_cnt = scrapy.Field()


class GameViewer(scrapy.Item):

    mid = scrapy.Field()
    date = scrapy.Field()
    visit = scrapy.Field()
    home = scrapy.Field()
    visit_score = scrapy.Field()
    home_score = scrapy.Field()
    link = scrapy.Field()
    viewer_min = scrapy.Field()
    viewer_max = scrapy.Field()
    viewer_avg = scrapy.Field()


