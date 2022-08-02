import asyncio

class StatusFinder:
    def __init__(self, crawlerprocess):
        self.settings = crawlerprocess.settings
        self.spider_mongo = crawlerprocess.spider_mongo

    async def get_user_status_spiders(self):
        user_status = self.settings.get('SPIDER_USER_STATUS', [])
        user_status_spiders = await self.spider_mongo.coll_spider_find({'status':{'$in':user_status}}, {'_id':0, 'spidername':1, 'status':1})
        return user_status_spiders
