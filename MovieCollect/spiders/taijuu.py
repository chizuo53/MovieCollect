import json
import re
from urllib.parse import urljoin

from scrapy import Spider, Request

from MovieCollect.items import MovieItem, MovieLinkItem
from MovieCollect.custom.utils.misc import hash_with_timestamp_random

class taijuu(Spider):
    name = 'taijuu'
#    sourcesite = '泰剧网'
#    sourcesite_url = 'https://www.taijuu.com/'
    custom_settings = {'REDIRECT_ENABLED':True, 'DOWNLOAD_TIMEOUT':3, 'RETRY_TIMES':1}

#    start_urls = ['https://www.taijuu.com/list/oumeiju.html',
#                  'https://www.taijuu.com/list/dongzuopian.html',
#                  'https://www.taijuu.com/list/xijupian.html',
#                  'https://www.taijuu.com/list/aiqingpian.html',
#                  'https://www.taijuu.com/list/kehuanpian.html',
#                  'https://www.taijuu.com/list/kongbupian.html',
#                  'https://www.taijuu.com/list/juqingpian.html',
#                  'https://www.taijuu.com/list/zhanzhengpian.html',
#            ]
    start_urls = ['https://www.taijuu.com/list/zhanzhengpian.html']


    def parse(self, response):
        url_prefix, url_suffix = response.url.rsplit('.', 1)
        page_list = response.css('.padding-0').xpath('./ul/li')
        first_page = int(page_list[0].xpath('./a/@href').re_first(r'/list/\w*-(\d*)\.html'))
        last_page = int(page_list[-1].xpath('./a/@href').re_first(r'/list/\w*-(\d*)\.html'))
        for page in range(first_page, last_page+1):
            movie_list_url = url_prefix + '-' + str(page) + '.' + url_suffix
            yield Request(movie_list_url, callback=self.parse_movie_list)

    def parse_movie_list(self, response):
        movie_li_list = response.css('.padding-0').css('.stui-vodlist').xpath('./li')
        for ml in movie_li_list:
            movie_post = [urljoin(response.url, ml.css('.stui-vodlist__thumb').attrib['data-original'])]
            movie_url = urljoin(response.url, ml.css('.stui-vodlist__detail').xpath('.//a').attrib['href'])
            movie_name = ml.css('.stui-vodlist__detail').xpath('.//a/text()').get()
            movie_identity = hash_with_timestamp_random(movie_url)

            movie_item = MovieItem(spidername=self.name, moviename=movie_name, movieurl=movie_url, movieidentity=movie_identity, image_urls=movie_post)
            yield movie_item
            yield Request(movie_url, callback=self.parse_movie, meta={'movie_identity':movie_identity})

    def parse_movie(self, response):
        movie_identity = response.meta['movie_identity']

        player_div_list = response.css('.padding-0').xpath('./div')       
        movie_players = player_div_list[1:-2]

        for mp in movie_players:
            player_name = mp.css('.stui-pannel__head').xpath('./h3/text()').get()
            player_link_list = mp.css('.stui-content__playlist').xpath('./li')
            for pl in player_link_list:
                link_url = urljoin(response.url, pl.xpath('./a').attrib['href'])
                link_name = pl.xpath('./a/text()').get()
                yield Request(link_url, callback=self.parse_playlink_m3u8, meta={'movie_identity':movie_identity, 'player_name':player_name, 'link_name':link_name, 'link_url':link_url})

    def parse_playlink_m3u8(self, response):
        m3u8_file = json.loads(response.css('.stui-player__video').xpath('./script/text()').re_first(r'player_aaaa=(.*)').strip())['url']
        m3u8_file = urljoin(response.url, m3u8_file)
        yield Request(m3u8_file, callback=self.parse_first_segment, errback=self.invalid_playlink, meta=response.meta)

    def parse_first_segment(self, response):
        if '#EXT-X-STREAM-INF' in response.text:
            m3u8_file = urljoin(response.url, re.search(r'#EXT-X-STREAM-INF.*\n(.*m3u8)', response.text).group(1))
            yield Request(m3u8_file, callback=self.parse_first_segment, errback=self.invalid_playlink, meta=response.meta)
        else:
            first_segment = urljoin(response.url, re.search(r'#EXTINF.*\n(.*)\n', response.text).group(1))
            yield Request(first_segment, callback=self.valid_playlink, errback=self.invalid_playlink, meta=response.meta)

    def valid_playlink(self, response):
        meta = response.meta
        movieidentity, playername, linkname, linkurl = meta['movie_identity'], meta['player_name'], meta['link_name'], meta['link_url']
        valid = True
        movielink_item = MovieLinkItem(movieidentity=movieidentity, playername=playername, linkname=linkname, linkurl=linkurl, valid=valid)
        yield movielink_item


    def invalid_playlink(self, failure):
        meta = failure.request.meta
        movieidentity, playername, linkname, linkurl = meta['movie_identity'], meta['player_name'], meta['link_name'], meta['link_url']
        valid = False
        movielink_item = MovieLinkItem(movieidentity=movieidentity, playername=playername, linkname=linkname, linkurl=linkurl, valid=valid)
        yield movielink_item









