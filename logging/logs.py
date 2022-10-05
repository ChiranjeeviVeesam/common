from logging import getLogger, config
from utility import get_config_details
from os.path import join

log_path = get_config_details(section="common", option="log_path")
log_config_path = get_config_details(section="common", option="log_config_path")
log_level = get_config_details(section = "common", option="log_level")

config.fileConfig(join(log_config_path,"logging.conf"), defaults = {"logfilename":join(log_path,"python-common-messages")})

def get_logger(name):
    __logger = getLogger(name)
    __logger.setLevel(log_level)
    return __logger
