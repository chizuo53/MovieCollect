import logging
import pprint
import os

from twisted.internet import defer, task
from twisted.python.failure import Failure
from scrapy import signals, Spider
from scrapy.extension import ExtensionManager
from scrapy.settings import overridden_settings, Settings
from scrapy.signalmanager import SignalManager
from scrapy.utils.defer import deferred_from_coro
from scrapy.utils.misc import load_object
from scrapy.crawler import Crawler, CrawlerProcess
from scrapy.utils.log import get_scrapy_root_handler, _get_handler

from MovieCollect.custom.database import SpiderMongo
from MovieCollect.custom.crud_error_catcher import crud_error_catcher
from MovieCollect.custom.statusfinder import StatusFinder
from MovieCollect.custom.statusperformer import StatusPerformer
from MovieCollect.custom.utils.misc import create_dir
from MovieCollect.custom.utils.log import RootFilter, SpiderFilter, SpiderLogCounterHandler


logger = logging.getLogger(__name__)

class AutoCrawler(Crawler):
    def __init__(self, spidercls, settings=None):
        if isinstance(spidercls, Spider):
            raise ValueError('The spidercls argument must be a class, not an object')

        if isinstance(settings, dict) or settings is None:
            settings = Settings(settings)

        self.spidercls = spidercls
        self.settings = settings.copy()

        spider_log_handlers = []
        spider_log_filter = SpiderFilter(self.spidercls)
        spider_log_dir = os.path.join(self.settings.get('SPIDER_LOG_DIR'), self.spidercls.name)
        create_dir(spider_log_dir)

        spider_log = os.path.join(spider_log_dir, spidercls.name+'.log')
        if self.spidercls.custom_settings:        
            self.spidercls.custom_settings['LOG_FILE'] = spider_log
        else:
            self.spidercls.custom_settings = {'LOG_FILE':spider_log}
        self.spidercls.update_settings(self.settings)
        spider_log_handler = _get_handler(self.settings)
        spider_log_handler.addFilter(spider_log_filter)
        spider_log_handlers.append(spider_log_handler)
        logging.root.addHandler(spider_log_handler)

        if self.settings.get('SPIDER_LOG_ERROR', False):
            spider_error_log = os.path.join(spider_log_dir, 'error.log')
            self.spidercls.custom_settings['LOG_FILE'] = spider_error_log
            self.spidercls.custom_settings['LOG_LEVEL'] = 'ERROR'
            self.spidercls.update_settings(self.settings)
            spider_error_log_handler = _get_handler(self.settings)
            spider_error_log_handler.addFilter(spider_log_filter)
            spider_log_handlers.append(spider_error_log_handler)
            logging.root.addHandler(spider_error_log_handler)

        self.signals = SignalManager(self)
        self.stats = load_object(self.settings['STATS_CLASS'])(self)

        handler = SpiderLogCounterHandler(self, level=self.settings.get('LOG_LEVEL'))
        logging.root.addHandler(handler)

        d = dict(overridden_settings(self.settings))
        logger.info("Overridden settings:\n%(settings)s",
                    {'settings': pprint.pformat(d)}, extra={'crawler':self})

        self.__remove_handler = lambda: [logging.root.removeHandler(handler) for handler in spider_log_handlers]
        self.signals.connect(self.__remove_handler, signals.engine_stopped)

        lf_cls = load_object(self.settings['LOG_FORMATTER'])
        self.logformatter = lf_cls.from_crawler(self)
        self.extensions = ExtensionManager.from_crawler(self)

        self.settings.freeze()
        self.crawling = False
        self.spider = None
        self.engine = None
        self.terminate = False

    @defer.inlineCallbacks
    def stop(self):
        if not self.terminate:
            self.terminate = True
            yield super().stop()


class AutoCrawlerProcess(CrawlerProcess):
    """
    A class to run multiple scrapy crawlers in a process simultaneously.

    This class extends :class:`~scrapy.crawler.CrawlerRunner` by adding support
    for starting a :mod:`~twisted.internet.reactor` and handling shutdown
    signals, like the keyboard interrupt command Ctrl-C. It also configures
    top-level logging.

    This utility should be a better fit than
    :class:`~scrapy.crawler.CrawlerRunner` if you aren't running another
    :mod:`~twisted.internet.reactor` within your application.

    The CrawlerProcess object must be instantiated with a
    :class:`~scrapy.settings.Settings` object.

    :param install_root_handler: whether to install root logging handler
        (default: True)

    This class shouldn't be needed (since Scrapy is responsible of using it
    accordingly) unless writing scripts that manually handle the crawling
    process. See :ref:`run-from-script` for an example.
    """

    def __init__(self, settings=None, install_root_handler=True):
        root_handler_dir = os.path.split(settings.get('LOG_FILE'))[0]
        create_dir(root_handler_dir)
        super().__init__(settings, install_root_handler)

        root_handler = get_scrapy_root_handler()
        root_filter = RootFilter()
        root_handler.addFilter(root_filter)

        self.running_crawlers = {}
        self.spider_mongo = SpiderMongo(self.settings)
        self.status_finder = StatusFinder(self)
        self.status_performer = StatusPerformer(self)

        self._auto_crawl_interval = self.settings.get('AUTO_CRAWL_INTERVAL', 60)
        self._change_spider_status = False

        crud_error_catcher.initial(self)

    def run_spider_loop(self):
        tl = task.LoopingCall(self._run_spider_loop)
        tl.start(self._auto_crawl_interval)

    @defer.inlineCallbacks
    def _run_spider_loop(self):
        if self._change_spider_status:
            return
        logger.debug('Start to get spiders those are in user status')
        self._change_spider_status = True
        user_status_spiders = yield deferred_from_coro(self.status_finder.get_user_status_spiders())
        self.status_performer.perform(user_status_spiders)
        self._change_spider_status = False

    def stop(self):
        """
        Stops simultaneously all the crawling jobs taking place.

        Returns a deferred that is fired when they all have ended.
        """
        def close_mongo(_):
            self.spider_mongo.close
            return _

        d = defer.DeferredList([c.stop() for c in self.running_crawlers.values()])
        d.addBoth(close_mongo)
        return d


