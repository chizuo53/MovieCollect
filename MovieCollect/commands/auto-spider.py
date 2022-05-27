from scrapy.commands import BaseRunSpiderCommand
from scrapy.exceptions import UsageError


class Command(BaseRunSpiderCommand):

    requires_project = True

    def syntax(self):
        return "[options]"

    def short_desc(self):
        return "Run auto-spider"

    def run(self, args, opts):
        if len(args) != 0:
            raise UsageError("running 'scrapy auto-spider' with any argument is not supported")

        self.crawler_process.run_spider_loop()
        self.crawler_process.start()

        if (
            self.crawler_process.bootstrap_failed
            or hasattr(self.crawler_process, 'has_exception') and self.crawler_process.has_exception
        ):
            self.exitcode = 1

