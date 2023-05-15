import logging

from twisted.internet import defer
from scrapy.utils.defer import deferred_from_coro

logger = logging.getLogger(__name__)

class SpiderRater:

    def __init__(self, crawlerprocess):
        self.crawlerprocess = crawlerprocess
        self.settings = crawlerprocess.settings
        self.spider_mongo = crawlerprocess.spider_mongo
        self.min_rate = self.settings.get('SPIDER_MIN_CONCURRENTCY')
        self.max_rate = self.settings.get('SPIDER_MAX_CONCURRENTCY')

    @defer.inlineCallbacks
    def change_rate(self):
        running_spiders = list(self.crawlerprocess.running_crawlers.keys())
        running_spiders_rate = yield deferred_from_coro(self.spider_mongo.coll_spider_find({'spidername':{'$in':running_spiders}, 'status':'running'},{'spidername':1,'rate':1}))
        for spider in running_spiders_rate:
            spidername = spider['spidername']
            rate = int(spider['rate'])
            if not self.min_rate <= rate <= self.max_rate:
                logger.error(f'Cannot change Spider: {spidername} concurrency lower than {self.min_rate} or bigger than {self.max_rate}')
            elif spidername in self.crawlerprocess.running_crawlers:
                downloader = self.crawlerprocess.running_crawlers[spidername].engine.downloader
                if downloader.ip_concurrency != rate:
                    downloader.ip_concurrency = rate
                    for slot in downloader.slots.values():
                        slot.concurrency = rate
                    logger.info(f'Change Spider: {spidername} concurrency to {rate}')



