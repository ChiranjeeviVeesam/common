from calculator import Calculator
from logs import get_logger, get_log_func
#logging.basicConfig(filename='logs1.log', level=logging.DEBUG, format ='%(asctime)s:%(levelname)s:%(message)s')
#this won't create logs1.log file as default root logging configuration for imported class calculator is same for all
logger = get_logger(__name__)
log = get_log_func(__name__)
#logger.setLevel(logging.INFO)

#filehandler = logging.FileHandler('main.log')
#formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

#logger.addHandler(filehandler)
#filehandler.setFormatter(formatter)
class Main:
    __calc_client = Calculator()
    @log 
    def main(self):
        # logger.info('process started')
        self.__calc_client.addition(1, 2)
        # logger.info('process ended')

if __name__ == '__main__':
    Main().main()
