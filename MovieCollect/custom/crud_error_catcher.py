import logging
from functools import wraps

logger = logging.getLogger(__name__)

class CrudErrorCatcher:
    def __init__(self):
        self.error_count = 0
        self.error_messages = []



    def initial(self, crawlerprocess):
        self.crawlerprocess = crawlerprocess
        self.settings = crawlerprocess.settings

    def dcatcher(self, func):
        async def inner(*args, **kwargs):
            try:
                result = await func(*args, **kwargs)
            except Exception as e:
                self.error_count += 1
                self.error_messages.append(str(e))
                for k,v in vars(self.__class__).items():
                    if k.startswith('activate_') and callable(v):
                        getattr(self, k)(e, func, *args, **kwargs)
            else:
                return result
        return inner

    def activate_log(self, exp, func, *args, **kwargs):
        logger.critical(f'CRUD operation: {func.__name__} failed.', exc_info=exp)

    def activate_crud_record(self, exp, func, *args, **kwargs):
        crud_error_op_file = self.settings.get('CRUD_ERROR_OP_FILE')
        with open(crud_error_op_file, 'a') as f:
             f.write(f'{func.__name__}, {args}, {kwargs}')

    def activate_mail(self, exp, func, *args, **kwargs):
        pass

crud_error_catcher = CrudErrorCatcher()
