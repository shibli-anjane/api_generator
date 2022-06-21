import sys, logging
from generalapi_properties import *
from logging.handlers import TimedRotatingFileHandler


formatter_string='%(asctime)s: %(levelname)s: %(message)s'
FORMATTER = logging.Formatter(formatter_string)


def get_console_handler():
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setFormatter(FORMATTER)
   return console_handler

def get_file_handler(nme):
   file_handler = TimedRotatingFileHandler(LogDir+nme, when='midnight')
   file_handler.setFormatter(FORMATTER)
   return file_handler

def get_logger(logger_name):
   logger = logging.getLogger(logger_name)
   if (logger.handlers!=[]):
        logger.handlers=[]
   logger.setLevel(logging.DEBUG) # better to have too much log than not enough
   logger.addHandler(get_console_handler())
   logger.addHandler(get_file_handler(logger.name))

   # with this pattern, it's rarely necessary to propagate the error up to parent

   logger.propagate = False

   print(logger)

   return logger