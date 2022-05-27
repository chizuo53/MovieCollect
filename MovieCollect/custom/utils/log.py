import logging

class RootFilter(logging.Filter):
    def filter(self, record):
        if not getattr(record, 'spider', None) and not getattr(record, 'crawler', None):
            return True

class SpiderFilter(logging.Filter):
    def __init__(self, spidercls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spidercls = spidercls

    def filter(self, record):
        if (getattr(record, 'spider', None) and record.spider.__class__ is self.spidercls) or \
        (getattr(record, 'crawler', None) and record.crawler.spidercls is self.spidercls):
            return True

class SpiderLogCounterHandler(logging.Handler):
    """Record log levels count into a crawler stats"""

    def __init__(self, crawler, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.crawler = crawler

    def emit(self, record):
        if (getattr(record, 'spider', None) and record.spider.__class__ is self.crawler.spidercls) or \
        (getattr(record, 'crawler', None) and record.crawler is self.crawler):
            sname = f'log_count/{record.levelname}'
            self.crawler.stats.inc_value(sname)
