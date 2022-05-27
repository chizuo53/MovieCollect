from abc import ABC, abstractmethod
import logging
import os
import sys

from twisted.internet import defer
from scrapy.utils.defer import deferred_from_coro

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
    def check_crawler_running(self):
        if spidername not in self.crawlerprocess.crawlers:
            raise SpiderNotRunningError(f'Spider {spidername} is not found in crawlers.')
        elif self.crawlerprocess.crawlers[spidername].terminate or not self.crawlerprocess.crawlers[spidername].crawling:
            raise SpiderNotRunningError(f'Spider {spidername} is terminating.')


class start(Worker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spiderloader = self.crawlerprocess.spider_loader
        self.settings = self.crawlerprocess.settings

    @defer.inlineCallbacks
    def check_status(self, spidername):
        spidermodule_name = self.spiderloader.get_spidermodule_name(spidername)
        if spidermodule_name in sys.modules or spidername in self.crawlerprocess.crawlers:
            raise SpiderExistError('Spider :{spidername} is already imported or running.')
        yield deferred_from_coro(self.spiderloader.preload(spidername))

    @defer.inlineCallbacks
    def change_status(self, spidername):
        from MovieCollect.custom.crawler import AutoCrawler

        spider = self.spiderloader.load(spidername)
        crawler = AutoCrawler(self.settings)
        crawl_defer = crawler.crawl()

        def _done(result):
            if isinstance(result, Failure):
                status = 'error'
                message = result.getTraceback()
                LEVEL = 'ERROR'
            elif crawler.terminate:
                status = 'has_terminated'
                message = 'Spider: {spidername} has been terminated'
                LEVEL = 'INFO'
            else:
                status = 'finished'
                message = f'Finished crawling spider: {spidername}.'
                LEVEL = 'INFO'
            del self.crawlerprocess.crawlers[spidername]
            self._active.discard(crawl_defer)
            logger.log(LEVEL, message)
            yield deferred_from_coro(self.spider_mongo.change_spider_status(spidername, status, message))
            return result

        if getattr(crawl_defer, 'result', None) is not None and issubclass(crawl_defer.result.type, Exception):
            logger.error(crawler_defer.result.getTraceback())
            yield deferred_from_coro(self.spider_mongo.change_spider_status(spidername, 'error', crawler_defer.result.getTraceback()))
        else:
            message = f'Running spider: {spidername}.'
            self.crawlerprocess.crawlers[spidername] = crawler
            self.crawlerprocess._active.add(crawl_defer)
            logger.info(message)
            crawl_defer.addBoth(_done)
            yield deferred_from_coro(self.spider_mongo.change_spider_status(spidername, 'running', message))

        
class terminate(CheckCrawlerRunningMixin, Worker):
    def check_status(self, spidername):
        self.check_crawler_running()

    @defer.inlineCallbacks
    def change_status(self, spidername):
        message = f'Spider: {spidername} has been terminated'
        yield self.crawlerprocess.crawlers[spidername].stop()
        logger.info(message)
        yield deferred_from_coro(self.spider_mongo.change_spider_status(spidername, 'has_terminated', message))


class pause(CheckCrawlerRunningMixin, Worker):
    def check_status(self, spidername):
        self.check_crawler_running()
        if self.crawlerprocess.crawlers[spidername].engine.paused:
            raise SpiderNotRunningError(f'Spider :{spidername} is paused')

    async def change_status(self, spidername):
        message = f'Spider: {spidername} has been paused'
        self.crawlerprocess.crawlers[spidername].engine.pause()
        logger.info(message)
        await self.spider_mongo.change_spider_status(spidername, 'has_paused', message)


class resume(CheckCrawlerRunningMixin, Worker):
    def check_status(self, spidername):
        super().check_crawler_running()
        if not self.crawlerprocess.crawlers[spidername].engine.paused:
            raise SpiderNotRunningError(f'Spider :{spidername} is not paused')

    async def change_status(self, spidername):
        message = f'Spider: {spidername} has been resumed'
        self.crawlerprocess.crawlers[spidername].engine.unpause()
        logger.info(message)
        await self.spider_mongo.change_spider_status(spidername, 'running', message)


class restart(start):
    @defer.inlineCallbacks
    def check_status(self, spidername):
        if spidername in self.crawlerprocess.crawlers:
            raise SpiderExistError('Spider :{spidername} is running.')
        yield deferred_from_coro(self.spiderloader.preload(spidername))
    
    @defer.inlineCallbacks
    def change_status(self, spidername):
        spider_log_dir = os.path.join(self.settings.get('SPIDER_LOG_DIR'), spidername)
        spider_post_dir = os.path.join(self.settings.get('IMAGE_STORE'), spidername)
        delete_dir(spider_log_dir)
        delete_dir(spider_post_dir)
        yield deferred_from_coro(self.spider_mongo.delete_movie_by_spider(spidername))
        logger.info(f'Spider: {spidername} is restarting')
        yield super().change_status(spidername)


class delete(Worker):
    def __init__(self):
        super().__init__()
        self.spiderloader = self.crawlerprocess.spider_loader

    def check_status(self, spidername):
        if spidername in self.crawlerprocess.crawlers:
            raise SpiderExistError('Spider :{spidername} is running.')

    async def change_status(self, spidername):
        message = f'Spider: {spidername} has been deleted'
        spidermodule_name = self.spiderloader.get_spidermodule_name(spidername)
        if spidermodule_name in sys.modules:
            del sys.modules[spidermodule_name]
        spider_log_dir = os.path.join(self.settings.get('SPIDER_LOG_DIR'), spidername)
        spider_post_dir = os.path.join(self.settings.get('IMAGE_STORE'), spidername)
        delete_dir(spider_log_dir)
        delete_dir(spider_post_dir)
        await self.spider_mongo.delete_movie_by_spider(spidername)
        logger.info(message)
        await self.spider_mongo.change_spider_status(spidername, 'has_deleted', message)
        

