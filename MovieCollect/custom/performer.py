from abc import ABC, abstractmethod
import logging
import os
import sys

from twisted.python.failure import Failure
from twisted.internet import defer
from scrapy.utils.defer import deferred_from_coro

from MovieCollect.custom.db_importer import code_cacher
from MovieCollect.custom.utils.exceptions import SpiderExistError, SpiderNotRunningError
from MovieCollect.custom.utils.misc import delete_dir

logger = logging.getLogger(__name__)

class Worker(ABC):
    def __init__(self, crawlerprocess):
        self.crawlerprocess = crawlerprocess
        self.spider_mongo = crawlerprocess.spider_mongo

    @abstractmethod
    def check_status(self):
        pass

    @abstractmethod
    def change_status(spiders):
        pass

class CheckCrawlerRunningMixin:
    def check_crawler_running(self, spidername):
        if spidername not in self.crawlerprocess.running_crawlers:
            raise SpiderNotRunningError(f'Spider {spidername} is not found in running_crawlers.')
        elif self.crawlerprocess.running_crawlers[spidername].terminate or not self.crawlerprocess.running_crawlers[spidername].crawling:
            raise SpiderNotRunningError(f'Spider {spidername} is terminating.')


class start(Worker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spiderloader = self.crawlerprocess.spider_loader
        self.settings = self.crawlerprocess.settings

    @defer.inlineCallbacks
    def check_status(self, spidername):
        spidermodule_name = self.spiderloader.get_spidermodule_name(spidername)
        if spidermodule_name in sys.modules or spidername in self.crawlerprocess.running_crawlers:
            raise SpiderExistError(f'Spider :{spidername} is already imported or running.')
        yield deferred_from_coro(self.spiderloader.preload(spidername))


    @defer.inlineCallbacks
    def change_status(self, spidername):
        from MovieCollect.custom.crawler import AutoCrawler

        spider = self.spiderloader.load(spidername)
        crawler = AutoCrawler(spider, self.settings)
        crawl_defer = crawler.crawl()

        @defer.inlineCallbacks
        def _done(result):
            if isinstance(result, Failure):
                status = 'error'
                message = f'Error occured while running spider: {spidername}, error msg: result.getTraceback()'
                LEVEL = logging.ERROR
            elif crawler.terminate:
                status = 'has_terminated'
                message = f'Spider: {spidername} has been terminated'
                LEVEL = logging.INFO
            else:
                status = 'finished'
                message = f'Finished crawling spider: {spidername}.'
                LEVEL = logging.INFO
            del self.crawlerprocess.running_crawlers[spidername]
            self.crawlerprocess._active.discard(crawl_defer)
            logger.log(LEVEL, message)
            yield deferred_from_coro(self.spider_mongo.coll_spider_update_one({'spidername':spidername}, {'$set':{'status':status, 'comment':message}}, upsert=False))
            return result

        if getattr(crawl_defer, 'result', None) is not None and issubclass(crawl_defer.result.type, Exception):
            logger.error(f'Error occured when try to start spider: {spidername}, error msg: crawl_defer.result.getTraceback()')
            yield deferred_from_coro(self.spider_mongo.coll_spider_update_one({'spidername':spidername}, {'$set':{'status':'error', 'comment':crawl_defer.result.getTraceback()}}, upsert=False))
        else:
            message = f'Running spider: {spidername}.'
            self.crawlerprocess.running_crawlers[spidername] = crawler
            self.crawlerprocess._active.add(crawl_defer)
            logger.info(message)
            crawl_defer.addBoth(_done)
            yield deferred_from_coro(self.spider_mongo.coll_spider_update_one({'spidername':spidername}, {'$set':{'status':'running', 'comment':message}}, upsert=False))

        
class terminate(CheckCrawlerRunningMixin, Worker):
    def check_status(self, spidername):
        self.check_crawler_running(spidername)

    @defer.inlineCallbacks
    def change_status(self, spidername):
        yield self.crawlerprocess.running_crawlers[spidername].stop()
        logger.info(f'Spider: {spidername} has been terminated')


class pause(CheckCrawlerRunningMixin, Worker):
    def check_status(self, spidername):
        self.check_crawler_running(spidername)
        if self.crawlerprocess.running_crawlers[spidername].engine.paused:
            raise SpiderNotRunningError(f'Spider :{spidername} is paused')

    async def change_status(self, spidername):
        message = f'Spider: {spidername} has been paused'
        self.crawlerprocess.running_crawlers[spidername].engine.pause()
        logger.info(message)
        await self.spider_mongo.coll_spider_update_one({'spidername':spidername}, {'$set':{'status':'has_paused', 'comment':message}}, upsert=False)


class resume(CheckCrawlerRunningMixin, Worker):
    def check_status(self, spidername):
        super().check_crawler_running(spidername)
        if not self.crawlerprocess.running_crawlers[spidername].engine.paused:
            raise SpiderNotRunningError(f'Spider :{spidername} is not paused')

    async def change_status(self, spidername):
        message = f'Spider: {spidername} has been resumed'
        self.crawlerprocess.running_crawlers[spidername].engine.unpause()
        logger.info(message)
        await self.spider_mongo.coll_spider_update_one({'spidername':spidername}, {'$set':{'status':'running', 'comment':message}}, upsert=False)


class restart(start):
    @defer.inlineCallbacks
    def check_status(self, spidername):
        if spidername in self.crawlerprocess.running_crawlers:
            raise SpiderExistError('Spider :{spidername} is running.')
        yield deferred_from_coro(self.spiderloader.preload(spidername))
    
    @defer.inlineCallbacks
    def change_status(self, spidername):
        spider_log_dir = os.path.join(self.settings.get('SPIDER_LOG_DIR'), spidername)
        spider_post_dir = os.path.join(self.settings.get('IMAGES_STORE'), spidername)
        delete_dir(spider_log_dir)
        delete_dir(spider_post_dir)
        yield deferred_from_coro(self.spider_mongo.coll_movie_delete_many({'spidername':spidername}))
        logger.info(f'Spider: {spidername} is restarting')
        yield super().change_status(spidername)


class delete(Worker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spiderloader = self.crawlerprocess.spider_loader
        self.settings = self.crawlerprocess.settings

    def check_status(self, spidername):
        if spidername in self.crawlerprocess.running_crawlers:
            raise SpiderExistError('Spider :{spidername} is running.')

    async def change_status(self, spidername):
        message = f'Spider: {spidername} has been deleted'
        spidermodule_name = self.spiderloader.get_spidermodule_name(spidername)
        if spidermodule_name in sys.modules:
            del sys.modules[spidermodule_name]
        code_cacher.pop(spidername, None)
        spider_log_dir = os.path.join(self.settings.get('SPIDER_LOG_DIR'), spidername)
        spider_post_dir = os.path.join(self.settings.get('IMAGES_STORE'), spidername)
        delete_dir(spider_log_dir)
        delete_dir(spider_post_dir)
        await self.spider_mongo.coll_movie_delete_many({'spidername':spidername})
        logger.info(message)
        await self.spider_mongo.coll_spider_update_one({'spidername':spidername}, {'$set':{'status':'has_deleted', 'comment':message}}, upsert=False)
        

