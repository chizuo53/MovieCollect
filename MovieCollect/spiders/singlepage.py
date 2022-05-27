import json
from urllib.parse import urljoin

from scrapy import Spider

class singlepage(Spider):
    name = 'singlepage'
    start_urls = ['https://www.taijuu.com/v/16758.html']

    def parse(self, response):
        player_div_list = response.css('.padding-0').xpath('./div')
        movie_players = player_div_list[1:-2]

        for mp in movie_players:
            player_name = mp.css('.stui-pannel__head').xpath('./h3/text()').get()
            player_link_list = mp.css('.stui-content__playlist').xpath('./li')
            for pl in player_link_list:
                playlink_url = urljoin(response.url, pl.xpath('./a').attrib['href'])
                playlink_name = pl.xpath('./a/text()').get()
                print(json.dumps({'player_name':player_name, 'playlink_name':playlink_name, 'playlink_url':playlink_url}))
                print('++++++++++++++++++++++++++++++++++++++++++++')
