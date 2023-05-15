from scrapy.utils.project import get_project_settings

from MovieCollect.custom.crawler import AutoCrawlerProcess

def execute():
    settings = get_project_settings()
    autocp = AutoCrawlerProcess(settings)
    autocp.run_loop()
    autocp.start(stop_after_crawl=False)

