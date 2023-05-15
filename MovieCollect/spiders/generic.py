from scrapy import Spider as OriginSpider

from MovieCollect.custom.utils.misc import hash_with_timestamp_random

class Spider(OriginSpider):
    def __init__(self, name=None, **kwargs):
        if kwargs.get('spider_mongo', None):
            self.spider_mongo = kwargs.pop('spider_mongo')
        super().__init__(name, **kwargs)

    def hash_with_timestamp_random(self, movie_url):
        return hash_with_timestamp_random(movie_url)

    async def get_movie_identity_with_check(movie_name, movie_url):
        movie = await self.spider_mongo.coll_movie_find_one({'spidername':self.name, 'moviename':movie_name, 'movieurl':movie_url},{'movieidentity':1})
        if movie:
            exist = True
            movie_identity = movie['movieidentity']
        else:
            exist = False
            movie_identity = self.hash_with_timestamp_random(movie_url)
        return exist, movie_identity


    def parse_movie(self, response):
        raise NotImplementedError(f'spider: {self.name} does not implement parse_movie method')

    def get_search_request(self, moviename):
        raise NotImplementedError(f'spider: {self.name} does not implement get_search_request method')
