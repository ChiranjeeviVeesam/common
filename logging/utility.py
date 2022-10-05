from configparser import ConfigParser
from os.path import join, dirname 

def get_config_details(
    config_path = join(dirname(__file__),"..","config.cfg"),
    section = "common", option = "log_path"):
    config_object = import_configs(config_path)
    return config_object[section][option]

def import_configs(
    config_path = join(dirname(__file__),"..","config.cfg")
):
    config_object = ConfigParser() 
    config_object.read(config_path)
    return config_object
