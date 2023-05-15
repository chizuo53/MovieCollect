import time

from motor.motor_asyncio import AsyncIOMotorClient

from MovieCollect.custom.crud_error_catcher import crud_error_catcher
from MovieCollect.custom.utils import exceptions
from MovieCollect.custom.utils.misc import singleton

@singleton
class SpiderMongo:
    def __init__(self, settings):
        username = settings.get('MONGO_USER')
        password = settings.get('MONGO_PASSWORD')
        host = settings.get('MONGO_HOST', 'localhost')
        port = settings.get('MONGO_PORT', 27017)
        min_pool_size = settings.get('MONGO_MIN_POOLSIZE', 0)
        max_pool_size = settings.get('MONGO_MAX_POOLSIZE', 100)
        authenticate_db = settings.get('MONGO_AUTHDB', 'admin')
        spider_db = settings.get('SPIDER_DATABASE')
        spider_coll = settings.get('SPIDER_COLLECTION')
        movie_coll = settings.get('MOVIE_COLLECTION')
        tool_coll = settings.get('TOOL_COLLECTION')

        self.conn = AsyncIOMotorClient(host=host, port=port, username=username, password=password, authSource=authenticate_db, maxPoolSize=max_pool_size, minPoolSize=min_pool_size)

        self.spider_db = self.conn[spider_db]
        self.spider_coll = self.spider_db[spider_coll]
        self.movie_coll = self.spider_db[movie_coll]
        self.tool_coll = self.spider_db[tool_coll]

        self.func_cacher = {}

    def get_database(self):
        return self.spider_db

    def __getattr__(self, name):
        try:
            if name.startswith('coll'):
                _, collname, funcname = name.split('_', 2)
                if collname+':'+funcname not in self.func_cacher:
                    if collname+'_coll' in vars(self):
                        coll = vars(self)[collname+'_coll']
                        func = getattr(coll, funcname)
                        self.func_cacher[collname+':'+funcname] = crud_error_catcher.dcatcher(func)
                    else:
                        raise
                return self.func_cacher[collname+':'+funcname]
            raise
        except:
            return super().__getattr__(name)

    def close(self):
        self.conn.close
