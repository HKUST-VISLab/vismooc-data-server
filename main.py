'''The main entry point
'''

import sys
import os
from os import listdir, makedirs
from os.path import isfile, join, exists, abspath
from datetime import datetime, timezone, timedelta

from mathematician.fetch_data import DownloadFileFromServer
from mathematician.pipe import PipeLine
from mathematician.Processor import FormatCourseStructFile, FormatEnrollmentFile,\
    FormatLogFile, FormatUserFile, ExtractRawData, DumpToDB
from mathematician import config


def app():
    # Hong Kong Time
    now = datetime.now(timezone(timedelta(hours=8)))
    dir_name = str(now.year) + '-' + str(now.month) + '-' + str(now.day - 1)
    dir_name = os.path.join(".", config.FilenameConfig.Data_dir, dir_name)
    if not exists(dir_name):
        makedirs(dir_name, exist_ok=True)
    start_time = datetime.now()
    download = DownloadFileFromServer(dir_name)
    file_names = download.get_files_to_be_processed(True)
    pipeline = PipeLine()
    pipeline.input_files(file_names).pipe(FormatCourseStructFile()).pipe(
        FormatEnrollmentFile()).pipe(FormatLogFile()).pipe(FormatUserFile()).pipe(
            ExtractRawData()).pipe(DumpToDB())
    pipeline.excute()
    print('spend time:' + str(datetime.now() - start_time))

if __name__ == "__main__":
    # init the config if config file is provided
    if len(sys.argv) >= 2:
        config.init_config(sys.argv[1])
    app()
