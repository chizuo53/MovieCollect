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
        spider_detail_coll = settings.get('SPIDER_DETAIL_COLLECTION')
        spider_movie_coll = settings.get('SPIDER_MOVIE_COLLECTION')
        crawl_status_coll = settings.get('CRAWL_STATUS_COLLECTION')

        self.conn = AsyncIOMotorClient(host=host, port=port, username=username, password=password, authSource=authenticate_db, maxPoolSize=max_pool_size, minPoolSize=min_pool_size)

        self.spider_db = self.conn[spider_db]
        self.spider_detail_coll = self.spider_db[spider_detail_coll]
        self.movie_coll = self.spider_db[spider_movie_coll]
        self.crawl_status_coll = self.spider_db[crawl_status_coll]

    def get_database(self):
        return self.spider_db

    def get_detail_collection(self):
        return self.spider_detail_coll

    def get_movie_collection(self):
        return self.movie_coll

    def get_crawl_status_collection(self):
        return self.crawl_status_coll

    async def find_start_status_spiders(self):
        docs = []
        async for doc in self.spider_detail_coll.find({'status':'start'}):
            docs.append(doc)
        return docs

    async def get_spider_detail(self, spidername):
        result = await self.spider_detail_coll.find_one({'spidername':spidername})
        return result

    async def get_crawlset(self):
        result = await self.crawl_status_coll.find_one({'storetype':'crawling_or_crawled_spider'})
        result = result['crawlset'] if result else []
        return result

    async def add_crawl(self, spidername):
        result = await self.crawl_status_coll.update_one({'storetype':'crawling_or_crawled_spider'}, {'$push':{'crawlset':spidername}}, upsert=True)

    @crud_error_catcher.dcatcher
    async def change_spider_status(self, spider_name, status, comment):
        result = await self.spider_detail_coll.update_one({'spidername':spider_name}, {'$set':{'status':status, 'comment':comment}}, upsert=False)
        return result

    @crud_error_catcher.dcatcher
    async def add_movie(self, movie_identity, spider_name,  movie_name, movie_url, post):
        result = await self.movie_coll.update_one({'movieidentity':movie_identity}, {'$set':{'spidername':spider_name, 'moviename':movie_name, 'movieurl':movie_url, 'post':post, 'created_time':time.time()}}, upsert=True)
        if not (result.acknowledged and (result.upserted_id or (result.matched_count and result.modified_count))):
            raise exceptions.MongoUpsertError('Failed to add movie')
        return result

    @crud_error_catcher.dcatcher
    async def add_movie_link(self, movie_identity, playername, linkname, linkurl, valid):
        playerfield = 'player'+'.'+playername+'.'+linkname
        result = await self.movie_coll.update_one({'movieidentity':movie_identity}, {'$set':{playerfield:{'linkurl':linkurl, 'valid':valid}}}, upsert=True)
        if not (result.acknowledged and (result.upserted_id or (result.matched_count and result.modified_count))):
            raise exceptions.MongoUpsertError('Failed to add movie link')
        return result

    @crud_error_catcher.dcatcher
    async def change_movie_link_valid(self, movie_identity, playername, linkname, valid):
        linkfield = 'player'+'.'+playername+'.'+linkname+'.valid'
        result = await self.movie_coll.update_one({'movieidentity':movie_identity}, {'$set':{linkfield:valid}}, upsert=False)
        return result 

    @crud_error_catcher.dcatcher
    async def get_spider_by_status(self, status):
        result = []
        async for sp in self.spider_detail_coll.find({'status':{'$in':status}}, {'_id':0, 'spidername':1, 'status':1}):
            result.append(sp)
        return result

    @crud_error_catcher.dcatcher
    async def delete_movie_by_spider(self, spidername):
        await self.movie_coll.delete_many({'spidername':spidername})

    @crud_error_catcher.dcatcher
    async def get_spider_code(self, spidername):
        result = await self.spider_detail_coll.find_one({'spidername':spidername}, {'_id':0, 'code':1})
        return result['code']

    def close(self):
        self.conn.close
