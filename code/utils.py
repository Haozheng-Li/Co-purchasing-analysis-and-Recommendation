import time
import logging

logging.basicConfig(level=logging.INFO)


def log(func):
    def inner(*args, **kwargs):
        begin_time = time.time()
        logging.info(' Begin func: {}'.format(func.__name__))
        result = func(*args, **kwargs)
        logging.info(' End func: {}, time consuming: {}s'.format(func.__name__, time.time()-begin_time))
        return result
    return inner
