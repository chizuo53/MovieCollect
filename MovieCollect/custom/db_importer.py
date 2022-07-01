from importlib.abc import MetaPathFinder, SourceLoader
from importlib.util import spec_from_loader
import sys

code_cacher = {}

database_module_prefix = 'database_'

class DBMetaFinder(MetaPathFinder):
    def __init__(self, database_module_prefix):
        self.database_module_prefix = database_module_prefix
        self.code_cacher = code_cacher

    def find_spec(self, fullname, path, target=None):
        if path or not fullname.startswith(self.database_module_prefix):
            return None
        loader = DBSourceLoader(self.database_module_prefix, self.code_cacher)
        return spec_from_loader(fullname, loader, is_package=loader.is_package(fullname))

class DBSourceLoader(SourceLoader):
    def __init__(self, database_module_prefix, mongo):
        self.database_module_prefix = database_module_prefix
        self.code_cacher = code_cacher

    def get_spidername(self, fullname):
        return fullname.split(self.database_module_prefix)[1]

    def get_data(self, data):
        pass

    def get_filename(self, fullname):
        spidername = self.get_spidername(fullname)
        return 'mongo://' + spidername

    def get_code(self, fullname):
        spidername = self.get_spidername(fullname)
        try:
            code = self.code_cacher.pop(spidername)
        except KeyError:
            raise ImportError(f'Module {fullname}({module_name}) can not be found in code_cacher')
        return code

database_meta_finder = DBMetaFinder(database_module_prefix)
sys.meta_path.insert(0, database_meta_finder)


import scrapy.utils.misc
import scrapy.core.scraper

def warn_on_generator_with_return_value_stub(spider, callable):
    pass

scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
