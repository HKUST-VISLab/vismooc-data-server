'''Logger for vismooc data server
'''
import logging
from datetime import datetime

VISMOOC_LOGGER = logging.getLogger("vismooc")
VISMOOC_LOGGER.setLevel(logging.INFO)

CONSOLE_HANDLER = logging.StreamHandler()
CONSOLE_HANDLER.setLevel(logging.INFO)

FORMATTER = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')

CONSOLE_HANDLER.setFormatter(FORMATTER)
VISMOOC_LOGGER.addHandler(CONSOLE_HANDLER)

def warn(msg):
    '''Print the Warning message from vismooc data server
    '''
    VISMOOC_LOGGER.warning(str(msg))

def info(msg):
    '''Print the Info message from vismooc data server
    '''
    VISMOOC_LOGGER.info(str(msg))

PROGRESS_LENGTH = 40
def progressbar(filename, progress, total):
    '''A text progress bar
    '''
    progress_per_total = int(progress) / int(total)
    progress_length = int(progress_per_total * PROGRESS_LENGTH)
    time_now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    str_template = "\r%s-vismooc-%s-[%s%s] %d%% | %s"
    print(str_template % (time_now, 'DOWNLOADING', '#' * progress_length,
                          ' ' * (PROGRESS_LENGTH - progress_length),
                          int(progress_per_total * 100), filename), end='', flush=True)
    if progress == total:
        print('\n', end='', flush=True)
