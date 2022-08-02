import logging
import sys
from importlib import import_module, reload as imreload
from pkgutil import iter_modules

from zope.interface import implementer

from scrapy.interfaces import ISpiderLoader
from scrapy.utils.spider import iter_spider_classes

from MovieCollect.custom.database import SpiderMongo
from MovieCollect.custom.db_importer import code_cacher, database_module_prefix
from MovieCollect.custom.utils.exceptions import SpiderNotFoundError

logger = logging.getLogger(__name__)


@implementer(ISpiderLoader)
class SpiderLoader:
    """
    SpiderLoader is a class which locates and loads spiders
    in a Scrapy project.
    """

    def __init__(self, settings):
        self.spider_mongo = SpiderMongo(settings)
        self._spiders = {}

    def get_spidermodule_name(self, spidername):
        return database_module_prefix + spidername

    async def update_code_cacher(self, spidername):
        spider_code = await self.spider_mongo.coll_spider_find_one({'spidername':spidername},{'code':1})
        code_cacher[spidername] = spider_code['code']

    async def preload(self, spidername):
        await self.update_code_cacher(spidername)
        spidermodule_name = self.get_spidermodule_name(spidername)
        if spidermodule_name in sys.modules:
            imreload(sys.modules[spidermodule_name])
        else:
            import_module(spidermodule_name)
        spidermodule = sys.modules[spidermodule_name]
        spider = getattr(spidermodule, spidername)
        assert (spider in iter_spider_classes(spidermodule) and spider.name == spidername)

    @classmethod
    def from_settings(cls, settings):
        return cls(settings)

    def load(self, spidername):
        """
        Return the Spider class for the given spider name. If the spider
        name is not found, raise a KeyError.
        """
        spidermodule_name = self.get_spidermodule_name(spidername)
        try:
            return getattr(sys.modules[spidermodule_name], spidername)
        except KeyError:
            raise KeyError(f"Spider not found: {spidername}")

    def find_by_request(self, request):
        """
        Return the list of spider names that can handle the given request.
        """
        return []

    def list(self):
        """
        Return a list with the names of all spiders available in the project.
        """
        return []
