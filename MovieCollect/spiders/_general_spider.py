from scrapy import Spider

class _general_spider(Spider):
    update_requests = []

    custom_settings = {}

    def start_requests(self):
        for ur in self.update_requests:
            yield ur
