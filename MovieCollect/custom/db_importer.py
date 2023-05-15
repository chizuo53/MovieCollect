from importlib.abc import MetaPathFinder, SourceLoader
from importlib.util import spec_from_loader
import sys

code_cacher = {}

database_module_prefix = 'database_'

filename_prefix = 'mongo-'

class DBMetaFinder(MetaPathFinder):
    def __init__(self, database_module_prefix):
        self.database_module_prefix = database_module_prefix
        self.code_cacher = code_cacher

    def find_spec(self, fullname, path, target=None):
        if path or not fullname.startswith(self.database_module_prefix):
            return None
        loader = DBSourceLoader(self.database_module_prefix)
        return spec_from_loader(fullname, loader, is_package=loader.is_package(fullname))

class DBSourceLoader(SourceLoader):
    def __init__(self, database_module_prefix):
        self.database_module_prefix = database_module_prefix
        self.code_cacher = code_cacher
        self.filename_prefix = filename_prefix

    def get_spidername(self, fullname):
        return fullname.split(self.database_module_prefix)[1]

    def get_data(self, path):
        spidername = path.split(self.filename_prefix)[1]
        try:
            code = self.code_cacher[spidername]
        except KeyError:
            raise OSError(f'Module {self.database_module_prefix+spidername}({spidername}) cannot be found in code_cacher')
        return bytes(code, 'utf8')

    def get_filename(self, fullname):
        spidername = self.get_spidername(fullname)
        return self.filename_prefix + spidername


database_meta_finder = DBMetaFinder(database_module_prefix)
sys.meta_path.insert(0, database_meta_finder)
