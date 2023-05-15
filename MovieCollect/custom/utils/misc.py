import hashlib
import os
import time
import random
import shutil

from scrapy.utils.python import to_bytes
from scrapy.utils.log import failure_to_exc_info

def singleton(cls):
    _instance = []
    def _get_instance(*args, **kwargs):
        if not _instance:
            _instance.append(cls(*args, **kwargs))
        return _instance[0]
    return _get_instance


def log_failure(msg, logger_to_log):
    def errback(failure):
        logger_to_log.error(msg, exc_info=failure_to_exc_info(failure))
    return errback

def hash_with_timestamp_random(string_to_hash):
    chars = 'abcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*(-_=+)'
    random_string = ''.join(random.sample(chars, 10))

    nstime = str(time.time_ns())

    return hashlib.sha1(to_bytes(string_to_hash+nstime+random_string)).hexdigest()


def create_dir(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)

def delete_dir(dirname):
    shutil.rmtree(dirname, ignore_errors=True)


