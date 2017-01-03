'''The main entry point
'''

import sys
import os
from os import makedirs
from os.path import exists, join
from datetime import datetime, timezone, timedelta
from mathematician.fetch_data import DownloadFileFromServer
from mathematician.pipe import PipeLine
from mathematician.Processor import ParseCourseStructFile, ParseEnrollmentFile,\
    ParseLogFile, ParseUserFile, ExtractRawData, DumpToDB
from mathematician import config
from mathematician.config import DBConfig as DBC, FilenameConfig as FC
from mathematician.DB import mongo_dbhelper

def get_offline_files():
    """Fetch meta db files data from db"""
    database = mongo_dbhelper.MongoDB(DBC.DB_HOST, DBC.DB_NAME, DBC.DB_PORT)
    metadbfiles = database.get_collection(DBC.COLLECTION_METADBFILES).find({})
    items = []
    for item in metadbfiles:
        if item.get(DBC.FIELD_METADBFILES_TYPE) == DBC.TYPE_MONGO:
            file_path = item.get(DBC.FIELD_METADBFILES_FILEPATH)
            file_path = file_path[:file_path.rindex(os.sep)]
            mongo_files = [join(file_path, FC.ACTIVE_VERSIONS), join(file_path, FC.STRUCTURES)]
        else:
            items.append(item)
    files = [item.get(DBC.FIELD_METADBFILES_FILEPATH) for item in items]
    return files + mongo_files

def app(offline=False):
    '''The main entry of our script
    '''
    # Hong Kong Time
    now = datetime.now(timezone(timedelta(hours=8)))
    dir_name = str(now.year) + '-' + str(now.month) + '-' + str(now.day - 1)
    dir_name = os.path.join(".", config.FilenameConfig.Data_dir, dir_name)
    if not exists(dir_name):
        makedirs(dir_name, exist_ok=True)
    start_time = datetime.now()
    if offline:
        file_names = get_offline_files()
    else:
        download = DownloadFileFromServer(dir_name)
        file_names = download.get_files_to_be_processed(True)
    pipeline = PipeLine()
    pipeline.input_files(file_names).pipe(ParseCourseStructFile(True)).pipe(
        ParseEnrollmentFile()).pipe(ParseLogFile()).pipe(ParseUserFile()).pipe(
            ExtractRawData()).pipe(DumpToDB())
    pipeline.excute()
    print('spend time:' + str(datetime.now() - start_time))

if __name__ == "__main__":
    # init the config if config file is provided
    if len(sys.argv) >= 2:
        config.init_config(sys.argv[1])
    app(True)
