'''Process enrollment data
'''
import re

from operator import itemgetter
from mathematician.pipe import PipeModule
from mathematician.config import DBConfig as DBc
from mathematician.logger import info
from mathematician.Processor.utils import try_parse_date, try_get_timestamp
from mathematician.Processor.Rawfile2MongoProcessor.constant import RD_DATA


class EnrollmentProcessor(PipeModule):
    '''The processor to process enrollment data
    '''
    order = 3
    ENROLL = "enroll"
    UNENROLL = "unenroll"
    action = {'1': ENROLL, '0': UNENROLL}
    pattern_date = "%Y-%m-%d %H:%M:%S"

    def __init__(self):
        super().__init__()
        self.processed_files = []
        self.enrollments = []

    def load_data(self, data_filenames):
        '''Load target file
        '''
        for filename in data_filenames:
            if '-student_courseenrollment-' in filename:
                with open(filename, 'r', encoding='utf-8') as file:
                    next(file)
                    raw_data = file.readlines()
                    self.processed_files.append(filename)
                    yield raw_data

    def process(self, raw_data, raw_data_filenames=None):
        info("Processing enrollment record")
        if raw_data_filenames is None:
            return raw_data

        data_to_be_processed = self.load_data(raw_data_filenames)
        if data_to_be_processed is None:
            return raw_data
        courses = raw_data[RD_DATA].get(DBc.COLLECTION_COURSE)
        users = raw_data[RD_DATA].get(DBc.COLLECTION_USER)
        if courses is None:
            raise Exception('The enrollment table depends on courses table, which is None')
        if users is None:
            raise Exception('The enrollment table depends on users table, which is None')

        for data in data_to_be_processed:
            for row in sorted(data, key=itemgetter(3)):
                row = row[:-1].split('\t')

                user_id = row[1]
                user = users.get(user_id)
                if user is None:
                    continue

                course_id = row[2]
                if ':' in course_id:
                    course_id = course_id[course_id.index(':') + 1:]
                course_id = re.sub(r'[\.|\/|\+]', '_', course_id)
                course = courses.get(course_id)
                if course is None:
                    continue

                enrollment = {
                    DBc.FIELD_ENROLLMENT_COURSE_ID: course_id,
                    DBc.FIELD_ENROLLMENT_USER_ID: user_id,
                    DBc.FIELD_ENROLLMENT_TIMESTAMP: try_get_timestamp(
                        try_parse_date(row[3], EnrollmentProcessor.pattern_date)),
                    DBc.FIELD_ENROLLMENT_ACTION: EnrollmentProcessor.action.get(row[4])
                }
                self.enrollments.append(enrollment)

                if enrollment[DBc.FIELD_ENROLLMENT_ACTION] == EnrollmentProcessor.ENROLL:
                    # fill user collection
                    user[DBc.FIELD_USER_COURSE_IDS].add(course_id)
                    # fill course collection
                    course[DBc.FIELD_COURSE_STUDENT_IDS].add(user_id)
                else:
                    course[DBc.FIELD_COURSE_STUDENT_IDS].discard(user_id)
                    user[DBc.FIELD_USER_COURSE_IDS].discard(course_id)
                    user[DBc.FIELD_USER_DROPPED_COURSE_IDS].add(course_id)

        processed_data = raw_data

        processed_data[RD_DATA][DBc.COLLECTION_ENROLLMENT] = self.enrollments
        processed_data[RD_DATA][DBc.COLLECTION_USER] = users
        processed_data[RD_DATA][DBc.COLLECTION_COURSE] = courses
        processed_data.setdefault('processed_files', []).extend(self.processed_files)

        return processed_data
