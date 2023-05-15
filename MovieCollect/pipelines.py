# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface


import logging
import hashlib
import os
import time
from itemadapter import ItemAdapter

from scrapy.utils.python import to_bytes
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem

from MovieCollect.items import MovieItem, MovieLinkItem

from MovieCollect.custom.database import SpiderMongo


logger = logging.getLogger(__name__)

class MovieImagesPipeline(ImagesPipeline):

    def file_path(self, request, response=None, info=None, *, item=None):
        spidername = item.get('spidername')
        image_guid = hashlib.sha1(to_bytes(request.url)).hexdigest()
        return f'{spidername}/full/{image_guid}.jpg'

    def item_completed(self, results, item, info):
        if results:
            if results[0][0]:
                image_path = results[0][1]['path']
            else:
                image_path = info.spider.settings.get('DEFAULT_POST_IMG')
            image_path = os.path.join('/static/post', image_path.lstrip('/'))
            ItemAdapter(item)[self.images_result_field] = image_path
        return item


class MoviePipeline:

    def open_spider(self, spider):
        self.spider_mongo = SpiderMongo(spider.settings)

    async def process_item(self, item, spider):
        if isinstance(item, MovieItem):
            movieidentity, spidername, moviename, movieurl, post = item['movieidentity'], spider.name, item['moviename'], item['movieurl'], item['images']
            result = await self.spider_mongo.coll_movie_update_one({'movieidentity':movieidentity}, {'$set':{'spidername':spidername, 'moviename':moviename, 'movieurl':movieurl, 'post':post, 'created_time':time.time()}}, upsert=True)
        return item


class MovieLinkPipeline(MoviePipeline):

    async def process_item(self, item, spider):
        if isinstance(item, MovieLinkItem):
            movieidentity, playername, linkname, linkurl, valid = item['movieidentity'], item['playername'], item['linkname'], item['linkurl'], item['valid']
            playerfield = 'player'+'.'+playername+'.'+linkname
            result = await self.spider_mongo.coll_movie_update_one({'movieidentity':movieidentity}, {'$set':{playerfield:{'linkurl':linkurl, 'valid':valid}}}, upsert=True)
        return item

