from logging import getLogger, config
from utility import get_config_details
from os.path import join
from functools import wraps 

log_path = get_config_details(section="common", option="log_path")
log_config_path = get_config_details(section="common", option="log_config_path")
log_level = get_config_details(section = "common", option="log_level")

config.fileConfig(join(log_config_path,"logging.conf"), defaults = {"logfilename":join(log_path,"python-common-messages.log")})

def get_logger(name):
    __logger = getLogger(name)
    __logger.setLevel(log_level)
    return __logger

def get_log_func(name):
    logger = getLogger(name)
    logger.setLevel(log_level)

    def __log(func):
        @wraps(func)
        def __decorated_function(self, *args, **kwargs):
            if func.__name__.startswith("_"):
                logger.debug("function %s is called" % func.__name__)
            else:
                logger.info("Function %s is called" % func.__name__)
            logger.debug("used arguments: %s" % str(args))
            logger.debug("used keywords: %s" % str(kwargs))

            try:
                result = func(self, *args, **kwargs)
                logger.debug("Result: %s" % str(result))
            except Exception as ex:
                logger.exception(ex)
                raise ex from ex
            
            if func.__name__.startswith("_"):
                logger.debug("Function %s is ended" % func.__name__)
            else:
                logger.info("Function %s is ended" % func.__name__)
            return result 
        return __decorated_function
    return __log

