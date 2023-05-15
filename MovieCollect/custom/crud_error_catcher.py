import logging
import os

from MovieCollect.custom.utils.misc import create_dir

logger = logging.getLogger(__name__)

class CrudErrorCatcher:
    def __init__(self):
        self.error_count = 0
        self.error_messages = []

    def initial(self, crawlerprocess):
        self.crawlerprocess = crawlerprocess
        self.settings = crawlerprocess.settings

        self.crud_error_dir = self.settings.get('CRUD_ERROR_DIR')
        create_dir(self.crud_error_dir)
        self.crud_read_file = os.path.join(self.crud_error_dir, 'read-op.txt')
        self.crud_write_file = os.path.join(self.crud_error_dir, 'write-op.txt')

    def dcatcher(self, func):
        async def inner(*args, **kwargs):
            try:
                if func.__name__ == 'find' or func.__name__ == 'aggregate':
                    results = []
                    async for result in func(*args, **kwargs):
                        results.append(result)
                else:
                    results = await func(*args, **kwargs)
            except Exception as e:
                self.error_count += 1
                self.error_messages.append(str(e))
                for k,v in vars(self.__class__).items():
                    if k.startswith('activate_') and callable(v):
                        getattr(self, k)(e, func, *args, **kwargs)
            else:
                return results
        return inner

    def activate_log(self, exp, func, *args, **kwargs):
        logger.critical(f'CRUD operation: {func.__name__} failed.', exc_info=exp)

    def activate_crud_record(self, exp, func, *args, **kwargs):
        if func.__name__.startswith('find'):
            crud_error_file = self.crud_read_file
        else:
            crud_error_file = self.crud_write_file
        with open(crud_error_file, 'a') as f:
             f.write(f'{func.__name__}, {args}, {kwargs}\n')

    def activate_mail(self, exp, func, *args, **kwargs):
        pass

crud_error_catcher = CrudErrorCatcher()
