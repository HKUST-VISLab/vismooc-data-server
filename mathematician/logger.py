'''Logger for vismooc data server
'''
import logging
from datetime import datetime
from os import path
from . import __version__

VISMOOC_LOGGER = logging.getLogger("vismooc-ds@"+__version__)
VISMOOC_LOGGER.setLevel(logging.DEBUG)

CONSOLE_HANDLER = logging.StreamHandler()
CONSOLE_HANDLER.setLevel(logging.DEBUG)

FORMATTER = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s', '%Y-%m-%d %H:%M:%S')

CONSOLE_HANDLER.setFormatter(FORMATTER)
VISMOOC_LOGGER.addHandler(CONSOLE_HANDLER)

def set_log_level(level):
    '''Set the logger level
    '''
    if isinstance(level, str):
        level = level.lower()
        if level == 'debug':
            VISMOOC_LOGGER.setLevel(logging.DEBUG)
            CONSOLE_HANDLER.setLevel(logging.DEBUG)
        elif level == 'info':
            VISMOOC_LOGGER.setLevel(logging.INFO)
            CONSOLE_HANDLER.setLevel(logging.INFO)
        elif level == 'warn':
            VISMOOC_LOGGER.setLevel(logging.WARNING)
            CONSOLE_HANDLER.setLevel(logging.WARNING)

def warn(msg):
    '''Print the Warning message from vismooc data server
    '''
    VISMOOC_LOGGER.warning(str(msg))

def error(msg):
    '''Print the Error message from vismooc data server
    '''
    VISMOOC_LOGGER.error(str(msg))

def info(msg):
    '''Print the Info message from vismooc data server
    '''
    VISMOOC_LOGGER.info(str(msg))

def debug(msg):
    '''Pring the Debug message from vismooc data server
    '''
    VISMOOC_LOGGER.debug(str(msg))

PROGRESS_LENGTH = 40
def progressbar(filename, progress, total):
    '''A text progress bar
    '''
    # begin from zero
    if progress < 0:
        total -= progress
        progress = 0
    progress_per_total = int(progress) / int(total)
    progress_length = int(progress_per_total * PROGRESS_LENGTH)
    time_now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    str_template = "\r%s-vismooc-%s-[%s%s] %d%%"
    if progress == 0:
        info("Begin to download:"+path.basename(filename))
    print(str_template % (time_now, 'DOWNLOADING', '#' * progress_length,
                          ' ' * (PROGRESS_LENGTH - progress_length),
                          int(progress_per_total * 100)), end='', flush=True)
    if progress == total:
        print('\n', end='', flush=True)
