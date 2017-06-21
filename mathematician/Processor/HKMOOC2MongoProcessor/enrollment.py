'''Process the enrollment data
'''
from mathematician.config import DBConfig as DBc
from mathematician.logger import info, warn
from mathematician.pipe import PipeModule
from mathematician.Processor.utils import (split, try_get_timestamp,
                                           try_parse_date)

from .constant import RD_DATA


class EnrollmentProcessor(PipeModule):
    '''Parse enrollment file
    '''
    order = 3
    ENROLL = "enroll"
    UNENROLL = "unenroll"
    action = {'1': ENROLL, '0': UNENROLL}

    def __init__(self):
        super().__init__()
        self.course_enrollment = None

    def load_data(self, raw_data):
        '''Load target file
        '''
        self.course_enrollment = raw_data.get('student_courseenrollment') or []

    def process(self, raw_data, raw_data_filenames=None):
        '''process the data
        '''
        info("Processing ParseEnrollmentFile")
        self.load_data(raw_data)
        if self.course_enrollment is None:
            return raw_data
        courses = raw_data[RD_DATA][DBc.COLLECTION_COURSE]
        users = raw_data[RD_DATA][DBc.COLLECTION_USER]
        print(raw_data[RD_DATA].keys())

        enrollments = []
        for enroll_item in self.course_enrollment:
            try:
                enrollment = {}
                records = split(enroll_item)
                user_id = records[5]
                course_id = records[1]
                course_id = course_id[course_id.index(':') + 1:]
                course_id = course_id.replace('.', '_')
                enrollment_time = try_get_timestamp(try_parse_date(records[2]))
                enrollment[DBc.FIELD_ENROLLMENT_USER_ID] = user_id
                enrollment[DBc.FIELD_ENROLLMENT_COURSE_ID] = course_id
                enrollment[DBc.FIELD_ENROLLMENT_TIMESTAMP] = enrollment_time
                enrollment[DBc.FIELD_ENROLLMENT_ACTION] = EnrollmentProcessor.action.get(records[3])
                enrollments.append(enrollment)
                # fill in user collection
                if enrollment[DBc.FIELD_ENROLLMENT_ACTION] == EnrollmentProcessor.ENROLL:
                    users[user_id][DBc.FIELD_USER_COURSE_IDS].add(course_id)
                    users[user_id][DBc.FIELD_USER_DROPPED_COURSE_IDS].discard(course_id)
                    courses[course_id][DBc.FIELD_COURSE_STUDENT_IDS].add(user_id)
                elif enrollment[DBc.FIELD_ENROLLMENT_ACTION] == EnrollmentProcessor.UNENROLL:
                    users[user_id][DBc.FIELD_USER_DROPPED_COURSE_IDS].add(course_id)
                    users[user_id][DBc.FIELD_USER_COURSE_IDS].discard(course_id)
                    courses[course_id][DBc.FIELD_COURSE_STUDENT_IDS].discard(user_id)
            except ValueError as ex:
                warn("In EnrollmentProcessor, cannot get the enrollment information of item:" +
                     enroll_item)
                warn(ex)
            except BaseException as ex:
                warn("In EnrollmentProcessor, cannot get the enrollment information of item:" +
                     enroll_item)
                warn(ex)

        processed_data = raw_data
        # course and users collection are completed
        processed_data['data'][DBc.COLLECTION_ENROLLMENT] = enrollments
        # processed_data['data'][DBC.COLLECTION_USER] = list(users.values())
        # processed_data['data'][DBC.COLLECTION_COURSE] = list(courses.value())
        return processed_data
