'''The main entry point
'''

import sys
import json
from os import listdir, mkdir
from os.path import isfile, join, exists
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
    dirname = str(now.year) + '-' + str(now.month) + '-' + str(now.day)
    dirname = join(config.FilenameConfig.Data_dir, dirname)
    if not exists(dirname):
        mkdir(dirname)
    start_time = datetime.now()
    download = DownloadFileFromServer(dirname)
    click_record = download.get_click_stream()
    db_record = download.get_mongodb_and_mysqldb_snapshot()
    db_record.extend(click_record)
    if db_record and len(db_record) > 0:
        with open(join(dirname, config.FilenameConfig.MetaDBRecord_Name), 'w') as file:
            file.write(json.dumps(db_record))
    file_names = [join(dirname, f)
                  for f in listdir(dirname) if isfile(join(dirname, f))]
    if exists(join(dirname, 'mongodb')):
        file_names.append(join(dirname, config.FilenameConfig.ACTIVE_VERSIONS))
        file_names.append(join(dirname, config.FilenameConfig.STRUCTURES))
    pipeLine = PipeLine()
    pipeLine.input_files(file_names).pipe(FormatCourseStructFile()).pipe(FormatEnrollmentFile()).pipe(
        FormatLogFile()).pipe(FormatUserFile()).pipe(ExtractRawData()).pipe(DumpToDB())

    pipeLine.excute()
    print('spend time:' + str(datetime.now() - start_time))
