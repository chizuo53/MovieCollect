import logging
import sys
from pkgutil import iter_modules
from importlib import import_module, reload as imreload

from zope.interface import implementer

from scrapy.interfaces import ISpiderLoader
from scrapy.utils.spider import iter_spider_classes
from scrapy.utils.defer import deferred_from_coro

from MovieCollect.custom.database import SpiderMongo
from MovieCollect.custom.utils.exceptions import SameSpiderNameError

logger = logging.getLogger(__name__)

@implementer(ISpiderLoader)
class SpiderLoader:
    """
    SpiderLoader is a class which locates and loads spiders
    in a Scrapy project.
    """

    def __init__(self, settings):
        self.spider_mongo = SpiderMongo(settings)
        self.spider_modules = settings.getlist('SPIDER_MODULES')
        self._spiders = {}

    async def load_start_spiders(self):
        self._spiders.clear()
        try:
            start_spiders = await self.spider_mongo.find_start_status_spiders()
        except:
            logger.error('Failed to get start spiders from database', exc_info=True)
        else:
            await self._load_start_spiders(start_spiders)
        finally:
            return self._spiders

    async def _load_start_spiders(self, start_spiders):
        if start_spiders:
            start_spiders = [spider['spidername'] for spider in start_spiders]
            for spm in self.spider_modules:
                await self._load_spcls_from_spider_module(spm, start_spiders)

    async def _load_spcls_from_spider_module(self, spider_module, start_spiders):
        try:
            module = import_module(spider_module)
        except ImportError as e:
            logger.error(f'Error while importing spider module(spider module directory) {spider_module}', exc_info=True)
        else:
            if hasattr(module, '__path__'):
                for _, subpath, ispkg in iter_modules(module.__path__):
                    if not ispkg and subpath in start_spiders:
                        try:
                            if not self._check_module_isloaded(spider_module, subpath):
                                spcls = self._load_module_and_check_spider(spider_module, subpath)
                            else:
                                spcls = self._load_module_and_check_spider(spider_module, subpath, need_reload=True)
                        except SameSpiderNameError as e:
                            logger.error(e.errmsg)
                        except Exception as e:
                            logger.error(f'Failed to import module {subpath}', exc_info=True)
                            fullpath = spider_module+'.'+subpath
                            if fullpath in sys.modules:
                                del sys.modules[fullpath]
                            try:
                                await self.spider_mongo.change_spider_status(subpath, 'error', f'{e}')
                            except Exception as me:
                                logger.error(f'Failed to change spider status to error and message to {e}, error message as below {me}')
                        else:
                            self._spiders[subpath] = spcls
                            
    
    def _check_module_isloaded(self, spider_module, subpath):
        fullpath = spider_module+'.'+subpath
        if subpath in self._spiders:
            loaded_path = self._spiders['subpath'].__package__
            raise SameSpiderNameError(f'Spider:{loaded_path} has already existed and loaded, gave up loading {fullpath}')
        if fullpath in sys.modules:
            return True
        else:
            return False

    def _load_module_and_check_spider(self, spider_module, subpath, need_reload=False):
        fullpath = spider_module+'.'+subpath
        if need_reload:
            submodule = sys.modules[fullpath]
            imreload(submodule)
        else:
            submodule = import_module(fullpath)
        spider = getattr(submodule, subpath, None)
        assert (spider in iter_spider_classes(submodule) and spider.name == subpath)
        return spider

    @classmethod
    def from_settings(cls, settings):
        return cls(settings)

    def load(self, spider_name):
        """
        Return the Spider class for the given spider name. If the spider
        name is not found, raise a KeyError.
        """
        try:
            return self._spiders[spider_name]
        except KeyError:
            raise KeyError(f"Spider not found: {spider_name}")

    def find_by_request(self, request):
        """
        Return the list of spider names that can handle the given request.
        """
        return [
            name for name, cls in self._spiders.items()
            if cls.handles_request(request)
        ]

    def list(self):
        """
        Return a list with the names of all spiders available in the project.
        """
        return list(self._spiders.keys())
