from twisted.internet import defer, task

from scrapy import signals
from scrapy.utils.defer import deferred_from_coro

from MovieCollect.items import MovieItem, MovieLinkItem

class MovieItemStats:

    def __init__(self, stats, spider_mongo, interval=60.0):
        self.stats = stats
        self.spider_mongo = spider_mongo
        self.interval = interval
        self.multiplier = 60.0 / self.interval
        self.task = None

    @classmethod
    def from_crawler(cls, crawler):
        interval = crawler.settings.getfloat('MOVIEITEMSTATS_INTERVAL')
        if not interval:
            raise NotConfigured
        o = cls(crawler.stats, crawler.spider_mongo, interval)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(o.item_scraped, signal=signals.item_scraped)
        return o

    @defer.inlineCallbacks
    def record(record, spider):
        movies = self.stats.get_value('movie_count', 0)
        movielinks = self.stats.get_value('movielink_count', 0)
        mrate = int((movies - self.moviesprev) * self.multiplier)
        mlrate = int((movielinks - self.movielinksprev) * self.multiplier)
        self.moviesprev, self.movielinksprev = movies, movielinks

        yield deferred_from_coro(self.spider_mongo.coll_spider_update_one({'spidername':spider.name},{'$set':{'stats':{'movierate':mrate,'movielinkrate':mlrate,'moviecount':movies,'movielinkcount':movielinks}}}))

    def spider_opened(self, spider):
        self.moviesprev = 0
        self.movielinksprev = 0

        self.task = task.LoopingCall(self.record, spider)
        self.task.start(self.interval)

    @defer.inlineCallbacks
    def spider_closed(self, spider, reason):
        elapsed_time_seconds = self.stats.get_value(elapsed_time_seconds, spider=spider)
        movies = self.stats.get_value(movie_count, spider=spider)
        movielinks = self.stats.get_value(movielink_count, spider=spider)
        mrate = int(movie_count/elapsed_time_seconds)
        mlrate = int(movielink_count/elapsed_time_seconds)

        yield deferred_from_coro(self.spider_mongo.coll_spider_update_one({'spidername':spider.name},{'$set':{'stats':{'movierate':mrate,'movielinkrate':mlrate,'moviecount':movies,'movielinkcount':movielinks}}}))

    def item_scraped(self, item, spider):
        if isinstance(item, MovieItem):
            self.stats.inc_value('movie_count', spider=spider)
        elif isinstance(item, MovieLinkItem):
            self.stats.inc_value('movielink_count', spider=spider)

