# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class MovieItem(scrapy.Item):

    movieidentity = scrapy.Field()
    spidername = scrapy.Field()
    moviename = scrapy.Field()
    movieurl = scrapy.Field()

    image_urls = scrapy.Field()
    images = scrapy.Field()


class MovieLinkItem(scrapy.Item):

    movieidentity = scrapy.Field()
    playername = scrapy.Field()
    linkname = scrapy.Field()
    linkurl = scrapy.Field()
    valid = scrapy.Field()
