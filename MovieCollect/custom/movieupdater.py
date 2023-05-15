import inspect
import logging
import importlib
import sys
  
from twisted.internet import defer
from twisted.python.failure import Failure
from scrapy.http import Request, Response
from scrapy.utils.defer import deferred_from_coro

logger = logging.getLogger(__name__)

class MovieUpdater:
    def __init__(self, crawlerprocess):
        self.crawlerprocess = crawlerprocess
        self.settings = crawlerprocess.settings
        self.spider_loader = crawlerprocess.spider_loader
        self.spider_mongo = crawlerprocess.spider_mongo

        self.creating_crawler = False
        self.ready = False
        self.finished_spiders = []
        self.spider_cacher = {}
        self.searchable_spiders_cacher = {}
        self.updating_movies = set()
        self._general_spidercls = None
        self.crawler = None

    async def initial(self):
        self._general_spidercls = self._get_general_spidercls()
        finished_spiders = await self.spider_mongo.coll_spider_find({'status':'finished'}, {'spidername':1})
        if not finished_spiders:
            self.crawlerprocess.movie_updater = None
            logger.info('No finished spider found, destroy movieupdater')
            return
        for fs in finished_spiders:
            self.finished_spiders.append(fs['spidername'])
        await self.get_searchable_spiders()

    def _get_general_spidercls(self):
        from scrapy.spiders import Spider
        path = self.settings.get('GENERAL_SPIDER')
        spider_modulename, spider_classname = path.rsplit('.', 1)
        if spider_modulename not in sys.modules:
            module = importlib.import_module(spider_modulename)
        else:
            importlib.reload(sys.modules[spider_modulename])
            module = sys.modules[spider_modulename]
        spidercls = getattr(module, spider_classname)
        assert (
            inspect.isclass(spidercls)
            and issubclass(spidercls, Spider)
        )
        spidercls.name = spider_classname
        return spidercls

    async def get_movie_spider(self, spidername):
        if spidername not in self.spider_cacher:
            await self.spider_loader.preload(spidername)
            spidercls =  self.spider_loader.load(spidername)
            self.spider_cacher[spidername] = spidercls(spider_mongo=self.spider_mongo)
        return self.spider_cacher[spidername]
    
    def get_new_movies(self, movies):
        new_movies = set(movies) - self.updating_movies
        self.updating_movies = set(movies)
        return new_movies

    async def get_searchable_spiders(self):
        spider_entries = await self.spider_mongo.coll_spider_find({'status':'finished', 'searchable':True}, {'_id':0,'spidername':1})
        for se in spider_entries:
            spider = await self.get_movie_spider(se['spidername'])
            self.searchable_spiders_cacher[se['spidername']] = spider

    async def update_movies(self, movies):
        if not self.creating_crawler:
            new_movies = self.get_new_movies(movies)
            if not new_movies:
                return
            self.creating_crawler = True
            update_requests = await self.get_update_requests(new_movies)
            self._general_spidercls.update_requests = update_requests
            logger.info(f'Create crawler to scrape new requests: {update_requests}')
            self._crawl()
        elif self.ready == False or not self.crawler.engine.slot or self.crawler.engine.spider_is_idle(self.crawler.spider):
            logger.info('Ignore requests because MovieUpdater is not ready or crawler is idle')
            return
        else:
            new_movies = self.get_new_movies(movies)
            if not new_movies:
                return
            update_requests = await self.get_update_requests(new_movies)
            logger.info(f'Use crawler which is already exists to scrape new requests: {update_requests}')
            for ur in update_requests:
                self.crawler.engine.crawl(ur, self.crawler.spider)

    def _crawl(self):
        from MovieCollect.custom.crawler import AutoCrawler
        spidername = self._general_spidercls.name
        self.crawler = crawler = AutoCrawler(self._general_spidercls, self.settings)
        crawl_defer = crawler.crawl()

        @defer.inlineCallbacks
        def _done(result):
            if isinstance(result, Failure):
                status = 'error'
                message = f'Error occured when running spider: {spidername}, error msg: {result.getTraceback()}'
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
            yield deferred_from_coro(self.spider_mongo.coll_tool_update_one({'name':'movies_to_update'}, {'$pullAll':{'funcfield':list(self.updating_movies)}}))
            logger.log(LEVEL, message)
            self.crawlerprocess.movie_updater = None
            return result

        if getattr(crawl_defer, 'result', None) is not None and issubclass(crawl_defer.result.type, Exception):
            self.crawlerprocess.movieupdater = None
            logger.error(f'Spider: {spidername} bootstrap failed, error msg: {crawl_defer.result.getTraceback()}')
        else:
            self.ready = True
            message = f'Running spider: {spidername}.'
            self.crawlerprocess.running_crawlers[spidername] = crawler
            self.crawlerprocess._active.add(crawl_defer)
            logger.info(message)
            crawl_defer.addBoth(_done)

    async def get_update_requests(self, movies):
        movies = list(movies)
        movie_entries = await self.spider_mongo.coll_movie_aggregate([{'$match':{'moviename':{'$in':movies}}},{'$group':{'_id':'$moviename', 'info':{'$push':{'spidername':'$spidername','movieidentity':'$movieidentity', 'movieurl':'$movieurl'}}}}])
        update_requests = []

        for me in movie_entries:
            moviename = me['_id']
            moviebriefs = me['info']

            spider_used = []
            for mb in moviebriefs:
                spidername = mb['spidername']
                movieidentity = mb['movieidentity']
                movieurl = mb['movieurl']
                if spidername in self.finished_spiders:
                    spider = await self.get_movie_spider(spidername)
                    spider_used.append(spidername)
                    request = Request(movieurl, callback=spider.parse_movie, dont_filter=False, meta={'movie_identity':movieidentity})
                    update_requests.append(request)
            for sname, sspider in self.searchable_spiders_cacher.items():
                if sname not in spider_used:
                    if hasattr(sspider, 'get_search_request'):
                        srequest = sspider.get_search_request(moviename)
                        update_requests.append(srequest)
                    else:
                        logger.error(f'Spider: {sname} is searchable but lack of get_search_requests method')
            movies.remove(moviename)

        for movie in movies:
            for sname, sspider in self.searchable_spiders_cacher.items():
                if hasattr(sspider, 'get_search_request'):
                    srequest = sspider.get_search_request(movie)
                    update_requests.append(srequest)
                else:
                    logger.error(f'Spider: {sname} is searchable but lack of get_search_requests method')

        return update_requests



