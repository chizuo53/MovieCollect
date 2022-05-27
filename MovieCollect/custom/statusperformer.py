import logging

from twisted.internet import defer
from scrapy.utils.defer import deferred_from_coro, deferred_f_from_coro_f
from scrapy.utils.log import failure_to_exc_info

from MovieCollect.custom import performer

logger = logging.getLogger(__name__)

class StatusPerformer:
    def __init__(self, crawlerprocess):
        self.crawlerprocess = crawlerprocess
        self.spider_mongo = crawlerprocess.spider_mongo
        self.status_workers = {}

    def get_worker(self, workername):
        worker = getattr(performer, workername, None)
        if worker:
            assert issubclass(worker, performer.Worker)
            return worker
        raise AttributeError(f'Worker {workername} not exists.')

    @defer.inlineCallbacks
    def dispatch(self, status, spidername):

        @defer.inlineCallbacks
        def deal_with_error(ret, spidername, status):
            logger.error('Failed to change spider: {spidername} status to {status}, error message as: {errmsg}', exc_info=failure_to_exc_info(ret))
            yield deferred_from_coro(self.spider_mongo.change_spider_status(spidername, 'error', res.getTrackback()))
            return ret

        def discard_ret(_, spidername):
            return deferred_from_coro(self.status_workers[status].change_status(spidername))

        if status not in self.status_workers:
            StatusWorker = self.get_worker(status)
            statusworker = Statusworker(self.crawlerprocess)
            self.status_workers[status] = statusworker

        d = deferred_from_coro(self.status_workers[status].check_status(spidername))
        d.addCallback(discard_ret, spidername)
        d.addErrback(deal_with_error, spidername, status)



    @defer.inlineCallbacks
    def perform(self, spider_status):

        def perform_log(result, spidername, status):
            if isinstance(result, Failure):
                logger.error(f'StatusPerformer failed to perform {spidername} status to {status}')
            else:
                logger.info(f'StatusPerformer finished performing {spidername} status to {status}')

        for spider in spider_status:
            spidername = spider['spidername']
            status = spider['status']
            logger.info(f'StatusPerformer start to perform {spidername} status to {status}')
            d = yield self.dispatch(status, spidername)
            d.addBoth(perform_log, spidername, status)
            

        

