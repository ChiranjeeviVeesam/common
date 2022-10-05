from calculator import calculator
import logging 
from logs import get_logger
#logging.basicConfig(filename='logs1.log', level=logging.DEBUG, format ='%(asctime)s:%(levelname)s:%(message)s')
#this won't create logs1.log file as default root logging configuration for imported class calculator is same for all
logger = get_logger('root')

#logger.setLevel(logging.INFO)

#filehandler = logging.FileHandler('main.log')
#formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

#logger.addHandler(filehandler)
#filehandler.setFormatter(formatter)

if __name__ == '__main__':
    logger.info('process started')
    print(calculator.addition(1, 2))
    logger.info('process ended')
   
