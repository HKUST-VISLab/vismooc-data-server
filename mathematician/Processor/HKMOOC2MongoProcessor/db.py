from mathematician.config import DBConfig as DBc
from mathematician.config import FilenameConfig as FC
from mathematician.logger import info
from mathematician.pipe import PipeModule

from .constant import RD_DB


class DumpToDB(PipeModule):
    '''Dump the processed data into database
    '''
    order = 998

    def __init__(self):
        super().__init__()
        self.need_drop_collections = [DBc.COLLECTION_COURSE, DBc.COLLECTION_ENROLLMENT,
                                      DBc.COLLECTION_USER, DBc.COLLECTION_VIDEO]

    def process(self, raw_data, raw_data_filenames=None):
        '''process the data
        '''
        info("Insert data to DB")
        db_data = raw_data['data']
        # cast from set to list
        courses = db_data.get(DBc.COLLECTION_COURSE)
        if courses:
            for course_info in courses.values():
                course_info[DBc.FIELD_COURSE_STUDENT_IDS] = list(
                    course_info[DBc.FIELD_COURSE_STUDENT_IDS])
                course_info[DBc.FIELD_COURSE_VIDEO_IDS] = list(
                    course_info[DBc.FIELD_COURSE_VIDEO_IDS])

        users = db_data.get(DBc.COLLECTION_USER)
        if users:
            for user in users.values():
                user[DBc.FIELD_USER_COURSE_IDS] = list(user[DBc.FIELD_USER_COURSE_IDS])
                user[DBc.FIELD_USER_DROPPED_COURSE_IDS] = list(user[DBc.FIELD_USER_DROPPED_COURSE_IDS])
        # castfrom dictory to list, removing id index
        for (key, value) in db_data.items():
            if isinstance(value, dict):
                db_data[key] = list(value.values())

        # insert to db
        database = raw_data.get(RD_DB)
        for collection_name in db_data:
            collection = database.get_collection(collection_name)
            if collection_name in self.need_drop_collections:
                collection.delete_many({})
            if db_data[collection_name] and len(db_data[collection_name]) > 0:
                collection.insert_many(db_data[collection_name])

        # mark log files
        metainfos = database.get_collection('metadbfiles')
        for filename in raw_data_filenames:
            etag = filename.get('etag')
            filename = filename.get('path')
            if FC.Clickstream_suffix in filename:
                metainfos.update_one({"etag": etag}, {'$set': {"processed": True}})
        return raw_data
