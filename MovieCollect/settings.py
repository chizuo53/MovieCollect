# Scrapy settings for MovieCollect project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'MovieCollect'

SPIDER_MODULES = ['MovieCollect.spiders']
NEWSPIDER_MODULE = 'MovieCollect.spiders'

TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'

SPIDER_LOADER_CLASS = 'MovieCollect.custom.mongospiderloader.SpiderLoader'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'MovieCollect (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'MovieCollect.middlewares.MoviecollectSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'MovieCollect.middlewares.MoviecollectDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'MovieCollect.pipelines.MovieImagesPipeline': 300,
    'MovieCollect.pipelines.MoviePipeline': 301,
    'MovieCollect.pipelines.MovieLinkPipeline': 302,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

#日志相关
LOG_ENABLED = True
LOG_FILE = '/tmp/log/main.log'
LOG_LEVEL = 'DEBUG'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
SPIDER_LOG_DIR = '/tmp/log/spider/'
SPIDER_LOG_ERROR = True

CRUD_ERROR_OP_FILE = '/tmp/log/crud_error.data'

#自定义配置
COMMANDS_MODULE = 'MovieCollect.commands'

#MOVIE_FAIL_DATA_DIR = '/tmp/movie_data/'

AUTO_CRAWL_INTERVAL = 10

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_USER = 'root'
MONGO_PASSWORD = '123456'
MONGO_AUTHDB = 'admin'
MONGO_MIN_POOLSIZE = 0
MONGO_MAX_POOLSIZE = 100
SPIDER_DATABASE = 'collect'
SPIDER_DETAIL_COLLECTION = 'spider'
SPIDER_MOVIE_COLLECTION = 'movie'
CRAWL_STATUS_COLLECTION = 'crawlstatus'

IMAGES_STORE = '/tmp/img/post'
DEFAULT_POST_IMG = 'default.jpg'

SPIDER_USER_STATUS = ['start', 'terminate', 'pause', 'resume', 'restart', 'delete']

CONCURRENT_ITEMS = 5
CONCURRENT_REQUESTS = 5
CONCURRENT_REQUESTS_PER_DOMAIN = 2
CONCURRENT_REQUESTS_PER_IP = 2

DOWNLOAD_DELAY = 0.25
REDIRECT_ENABLED = True
DOWNLOAD_TIMEOUT = 6
RETRY_TIMES = 1
