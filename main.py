'''The main entry point
'''

import sys
import json
import os
from os import listdir, makedirs
from os.path import isfile, join, exists, abspath
from datetime import datetime, timezone, timedelta

from mathematician.fetch_data import DownloadFileFromServer
from mathematician.pipe import PipeLine
from mathematician.Processor import FormatCourseStructFile, FormatEnrollmentFile,\
    FormatLogFile, FormatUserFile, ExtractRawData, DumpToDB
from mathematician import config

if __name__ == "__main__":
    # init the config if config file is provided
    if len(sys.argv) >= 2:
        config.init_config(sys.argv[1])
    # Hong Kong Time
    now = datetime.now(timezone(timedelta(hours=8)))
    dir_name = str(now.year) + '-' + str(now.month) + '-' + str(now.day)
    dir_name = os.path.join(abspath("."), config.FilenameConfig.Data_dir, dir_name)
    print(dir_name)
    if not exists(dir_name):
        makedirs(dir_name, exist_ok=True)
    start_time = datetime.now()
    download = DownloadFileFromServer(dir_name)
    click_record = download.get_click_stream()
    db_record = download.get_mongodb_and_mysqldb_snapshot()
    db_record.extend(click_record)
    if db_record and len(db_record) > 0:
        with open(join(dir_name, config.FilenameConfig.META_DB_RECORD), 'w') as file:
            file.write(json.dumps(db_record))
    file_names = [join(dir_name, f)
                  for f in listdir(dir_name) if isfile(join(dir_name, f))]
    if exists(join(dir_name, 'mongodb')):
        file_names.append(join(dir_name, config.FilenameConfig.ACTIVE_VERSIONS))
        file_names.append(join(dir_name, config.FilenameConfig.STRUCTURES))
    pipeLine = PipeLine()
    pipeLine.input_files(file_names).pipe(FormatCourseStructFile()).pipe(FormatEnrollmentFile()).pipe(
        FormatLogFile()).pipe(FormatUserFile()).pipe(ExtractRawData()).pipe(DumpToDB())

    pipeLine.excute()
    print('spend time:' + str(datetime.now() - start_time))
