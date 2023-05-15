class SpiderException(Exception):
    def __init__(self, errmsg):
        self.errmsg = errmsg

    def __str__(self):
        return self.errmsg

    __repr__ = __str__

class SameSpiderNameError(SpiderException):
    pass

class SpiderNotFoundError(SpiderException):
    pass

class SpiderExistError(SpiderException):
    pass

class SpiderNotRunningError(SpiderException):
    pass

class MongoUpsertError(SpiderException):
    pass
