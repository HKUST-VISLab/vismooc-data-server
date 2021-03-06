from datetime import datetime

from mathematician.config import DBConfig as DBc
from mathematician.logger import info
from mathematician.pipe import PipeModule
from mathematician.Processor.utils import try_get_timestamp, try_parse_course_id, try_parse_video_id

from ..utils import get_data_by_table


class CourseProcessor(PipeModule):
    '''Processe course table
    '''
    order = 0

    def __init__(self):
        super().__init__()
        self.sql_table = 'courses'
        self.courses = {}

    def load_data(self):
        '''Load target table
        '''
        data = get_data_by_table(self.sql_table)
        return data or None

    def process(self, raw_data, raw_data_filenames=None):
        '''Processe course record
        '''
        info("Processing course record")
        data_to_be_processed = self.load_data()
        if data_to_be_processed is None:
            return raw_data

        course_videos = get_data_by_table('course_video')
        stats_overall = get_data_by_table('stats_overall')[0]
        grades = get_data_by_table('grades')

        for row in data_to_be_processed:
            course = {}
            original_course_id = stats_overall[0]
            # course_id = row[1]
            course_id = try_parse_course_id(original_course_id)
            course[DBc.FIELD_COURSE_ORG] = row[4]
            course[DBc.FIELD_COURSE_ID] = course_id
            course[DBc.FIELD_COURSE_ORIGINAL_ID] = course_id
            course[DBc.FIELD_COURSE_NAME] = row[2]
            course[DBc.FIELD_COURSE_YEAR] = row[3]
            course[DBc.FIELD_COURSE_IMAGE_URL] = None
            course[DBc.FIELD_COURSE_ENROLLMENT_START] = None
            course[DBc.FIELD_COURSE_ENROLLMENT_END] = None
            course[DBc.FIELD_COURSE_INSTRUCTOR] = row[5].split(' ')
            course[DBc.FIELD_COURSE_STATUS] = None
            course[DBc.FIELD_COURSE_URL] = None
            course[DBc.FIELD_COURSE_DESCRIPTION] = row[6]
            course[DBc.FIELD_COURSE_METAINFO] = {}
            course[DBc.FIELD_COURSE_STARTTIME] = try_get_timestamp(row[7]) if isinstance(row[7], datetime) else None
            course[DBc.FIELD_COURSE_ENDTIME] = try_get_timestamp(row[8])  if isinstance(row[8], datetime) else None
            course[DBc.FIELD_COURSE_STUDENT_IDS] = set()# [x[1] for x in grades if x[2] == original_course_id]
            course[DBc.FIELD_COURSE_VIDEO_IDS] = [try_parse_video_id(x[2]) for x in course_videos if x[1] == row[1]]
            course[DBc.FIELD_COURSE_GRADES] = {} #{x[1] : float(x[4]) for x in grades if x[2] == original_course_id}
            self.courses[course_id] = course

        processed_data = raw_data
        processed_data['data'][DBc.COLLECTION_COURSE] = self.courses

        return processed_data
