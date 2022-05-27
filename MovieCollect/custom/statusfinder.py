class StatusFinder:
    def __init__(self, crawlerprocess):
        self.settings = crawlerprocess.settings
        self.spider_mongo = crawlerprocess.spider_mongo

    async def get_user_status_spiders(self):
        user_status = self.settings.get('SPIDER_USER_STATUS', [])
        user_status_spiders = await self.spider_mongo.get_spider_by_status(user_status)
        return user_status_spiders
