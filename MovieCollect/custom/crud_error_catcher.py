import logging
from functools import wraps

logger = logging.getLogger(__name__)

class CrudErrorCatcher:
    def __init__(self):
        pass

    def initial(self, crawlerprocess):
        self.crawlerprocess = crawlerprocess
        self.settings. = crawlerprocess.settings

    def dcatcher(self, func):
        @wraps(func)
        async def inner(*args, **kwargs):
            try:
                result =  await func(*args, **kwargs)
            except Exception as e:
                self.error_count += 1
                self.error_messages.append(e.message)
                for k,v in vars(self.__class__):
                    if k.startswith('activate_') and callable(v):
                        getattr(self, k)(e, func, *args, **kwargs)
            else:
                return result
        return inner

    def activate_log(self, exp, func, *args, **kwargs):
        logger.critical(f'CRUD operation: {func.__name__} failed.', exc_info=exp)

    def activate_crud_record(self, exp, func, *args, **kwargs):
        crud_error_op_file = self.settings.get('CRUD_ERROR_OP')
        with open(crud_error_op_file, 'a') as f:
             a.write(f'{func.__name}, {args}, {kwargs}')

    def activate_mail(self):
        pass

crud_error_catcher = CrudErrorCatcher()
