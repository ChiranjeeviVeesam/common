from logs import get_log_func, get_logger

logger = get_logger(__name__)
log = get_log_func(__name__)
# formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
# filehandler = logging.FileHandler('calculator.log')

# logger.addHandler(filehandler)
# filehandler.setFormatter(formatter)

#logging.basicConfig(filename='logs.log', level=logging.DEBUG, format ='%(asctime)s:%(levelname)s:%(message)s')
# the default level is warning
class Calculator:
    __result = 0
    @log
    def addition(self, a, b):
        self.__result = a+b
        # logger.debug('Add {} and {} = {}'.format(a, b, result))
        return self.__result 
    @log
    def substraction(self, a, b):
        self.__result = a-b
        # logger.debug('Sub {} and {} = {}'.format(a, b, result))
        return self.__result
