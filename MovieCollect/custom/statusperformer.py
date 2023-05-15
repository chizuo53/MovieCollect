import logging

from twisted.internet import defer
from twisted.python.failure import Failure
from scrapy.utils.defer import deferred_from_coro, maybeDeferred_coro
from scrapy.utils.log import failure_to_exc_info

from MovieCollect.custom import performer

logger = logging.getLogger(__name__)

class StatusPerformer:
    def __init__(self, crawlerprocess):
        self.crawlerprocess = crawlerprocess
        self.spider_mongo = crawlerprocess.spider_mongo
        self.spiders_in_processing = set()
        self.status_workers = {}

    def get_worker(self, workername):
        worker = getattr(performer, workername, None)
        if worker:
            assert issubclass(worker, performer.Worker)
            return worker
        raise AttributeError(f'Worker {workername} not exists.')

    def dispatch(self, status, spidername):

        @defer.inlineCallbacks
        def deal_with_error(failure, spidername, status):
            logger.error(f'Failed to change spider: {spidername} status to {status}', exc_info=failure_to_exc_info(failure))
            yield deferred_from_coro(self.spider_mongo.coll_spider_update_one({'spidername':spidername}, {'$set':{'status':'error', 'comment':failure.getTraceback()}}, upsert=False))
            return failure
        
        @defer.inlineCallbacks
        def discard_ret(_, spidername):
            yield deferred_from_coro(self.status_workers[status].change_status(spidername))

        if status not in self.status_workers:
            StatusWorker = self.get_worker(status)
            statusworker = StatusWorker(self.crawlerprocess)
            self.status_workers[status] = statusworker

        d = maybeDeferred_coro(self.status_workers[status].check_status, spidername)
        d.addCallback(discard_ret, spidername)
        d.addErrback(deal_with_error, spidername, status)
        return d

    def perform(self, spider_status):

        def perform_log(result, spidername, status):
            if isinstance(result, Failure):
                logger.error(f'StatusPerformer failed to perform {spidername} status to {status}')
            else:
                logger.info(f'StatusPerformer finished performing {spidername} status to {status}')

        for spider in spider_status:
            spidername = spider['spidername']
            if spidername not in self.spiders_in_processing:
                self.spiders_in_processing.add(spidername)
                status = spider['status']
                logger.info(f'StatusPerformer start to perform {spidername} status to {status}')
                d = self.dispatch(status, spidername)
                d.addBoth(perform_log, spidername, status)
                d.addBoth(lambda _:self.spiders_in_processing.discard(spidername))
            

        

