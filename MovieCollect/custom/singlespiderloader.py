import logging
import sys
from pkgutil import iter_modules
from importlib import import_module, reload as imreload

from zope.interface import implementer

from scrapy.interfaces import ISpiderLoader
from scrapy.utils.spider import iter_spider_classes

from MovieCollect.custom.utils.exceptions import SpiderNotFoundError

logger = logging.getLogger(__name__)

@implementer(ISpiderLoader)
class SpiderLoader:
    """
    SpiderLoader is a class which locates and loads spiders
    in a Scrapy project.
    """

    def __init__(self, settings):
        self.spider_modules = settings.getlist('SPIDER_MODULES')
        self._spiders = {}

    def get_spidermodule_name(self, spidername):
        for spm in self.spider_modules:
            module = import_module(spm)
            if hasattr(module, '__path__'):
                for _, subpath, ispkg in iter_modules(module.__path__):
                    if not ispkg and subpath == spidername:
                        return spm+'.'+spidername
        raise SpiderNotFoundError('Spider: {spidername} can not be found in spider_modules')

    def preload(self, spidername):
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

    def load(self, spider_name):
        """
        Return the Spider class for the given spider name. If the spider
        name is not found, raise a KeyError.
        """
        spidermodule_name = self.get_spidermodule_name(spidername)
        try:
            return getattr(sys.modules[spidermodule_name], spidername)
        except KeyError:
            raise KeyError(f"Spider not found: {spider_name}")

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
