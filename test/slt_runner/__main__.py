import os
import logging
from slt_runner.python_runner import SQLLogicPythonRunner
from slt_runner.logger import logger, loglevel

def set_log_level(level):
    if level < logging.INFO:
        logging.basicConfig(format='%(levelname)s %(msecs)s %(filename)s:%(lineno)d - %(message)s')
    else:
        logging.basicConfig(format='%(asctime)s - %(message)s')    
    logger.setLevel(level)

set_log_level(loglevel(os.environ.get('LOGLEVEL', 'NOTSET').upper()))

if __name__ == '__main__':
    logger.info('SQLLogicTest started.')
    SQLLogicPythonRunner().run()