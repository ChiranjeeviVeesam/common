import logging 
from logs import get_logger

logger = get_logger(__name__)
# formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
# filehandler = logging.FileHandler('calculator.log')

# logger.addHandler(filehandler)
# filehandler.setFormatter(formatter)

#logging.basicConfig(filename='logs.log', level=logging.DEBUG, format ='%(asctime)s:%(levelname)s:%(message)s')
# the default level is warning
class calculator:
    result = 0
    @staticmethod
    def addition(a, b):
        result = a+b
        logger.debug('Add {} and {} = {}'.format(a, b, result))
        return result 
    @staticmethod
    def substraction(a, b):
        result = a-b
        logger.debug('Sub {} and {} = {}'.format(a, b, result))
        return result
