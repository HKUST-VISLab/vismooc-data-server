from datetime import datetime

from mathematician.config import DBConfig as DBc
from mathematician.logger import info, warn
from mathematician.pipe import PipeModule
from mathematician.Processor.utils import split

from .constant import RD_DATA, RD_DB


class UserProcessor(PipeModule):
    '''Parse user information from db snapshot
    '''
    order = 2

    def __init__(self):
        super().__init__()
        self.users = None
        self.user_profile = {}
        self.user_roles = {}
        self.raw_user_profile = None
        self.user_info = None
        self.course_access_role = None

    def load_data(self, raw_data):
        '''Load target file
        '''
        self.raw_user_profile = raw_data.get('auth_userprofile') or []
        self.user_info = raw_data.get('auth_user')
        self.course_access_role = raw_data.get('student_courseaccessrole') or []
        database = raw_data[RD_DB]
        user_collection = database.get_collection(DBc.COLLECTION_USER).find({})
        self.users = {user[DBc.FIELD_USER_ORIGINAL_ID]: user for user in user_collection}
        for user in self.users.values():
            user[DBc.FIELD_USER_COURSE_IDS] = set(user[DBc.FIELD_USER_COURSE_IDS])
            user[DBc.FIELD_USER_DROPPED_COURSE_IDS] = set(user[DBc.FIELD_USER_DROPPED_COURSE_IDS])

    def process(self, raw_data, raw_data_filenames=None):
        '''process the data
        '''
        info("Processing ParseUserFile")
        self.load_data(raw_data)
        for record in self.raw_user_profile:
            fields = split(record)
            self.user_profile[fields[16]] = fields
        if self.user_info is None:
            warn("No auth_user table in SQL!")
            raw_data[RD_DATA][DBc.COLLECTION_USER] = self.users
            return raw_data

        for access_role in self.course_access_role:
            records = split(access_role)
            if len(records) < 3:
                continue
            course_id = records[2]
            try:
                course_id = course_id[course_id.index(':') + 1:]
            except ValueError as ex:
                warn("In ParseUserFile, cannot get courseId when try to get access role of "
                     "course: " + access_role)
                warn(ex)
            course_id = course_id.replace('.', '_')
            # records[4] - userId
            # records[3] instructor or admin?
            self.user_roles.setdefault(records[4], {}).setdefault(
                course_id, []).append(records[3])

        for record in self.user_info:
            try:
                user_fields = split(record)
                user_id = user_fields[0]
                user = self.users.get(user_id) or {}
                user_profile = self.user_profile.get(user_id)
                birth_year = datetime.strptime(user_profile[6], '%Y')\
                    if (user_profile and (user_profile[6] != "NULL" and
                                          len(user_profile[6]) == 4)) else None
                user[DBc.FIELD_USER_USER_NAME] = user_fields[4]
                user[DBc.FIELD_USER_LANGUAGE] = user_profile and user_profile[4]
                user[DBc.FIELD_USER_LOCATION] = user_profile and user_profile[5]
                user[DBc.FIELD_USER_BIRTH_DATE] = birth_year and birth_year.year
                user[DBc.FIELD_USER_EDUCATION_LEVEL] = user_profile and user_profile[8]
                user[DBc.FIELD_USER_GENDER] = user_profile and user_profile[7]
                user[DBc.FIELD_USER_COURSE_IDS] = set()
                user[DBc.FIELD_USER_DROPPED_COURSE_IDS] = set()
                user[DBc.FIELD_USER_BIO] = user_profile and user_profile[14]
                user[DBc.FIELD_USER_COUNTRY] = user_profile and (
                    user_profile[11] or user_profile[5])
                user[DBc.FIELD_USER_NAME] = user_fields[5] + user_fields[6]
                user[DBc.FIELD_USER_ORIGINAL_ID] = user_id
                user[DBc.FIELD_USER_COURSE_ROLE] = self.user_roles.get(user_id) or {}
                if user_fields[8] == 1:  # is_staff
                    info('It is a staff user_' + user_fields[4])
                    user[DBc.FIELD_USER_COURSE_ROLE]['*'] = ['staff']
                self.users[user_id] = user
            except BaseException as ex:
                warn("In ParseUserFile, cannot get the user information of record:" + record +
                     ", and userProfile:" + str(user_profile))
                warn(ex)
        # fill the course instractor of courses
        courses = raw_data[RD_DATA][DBc.COLLECTION_COURSE]
        for course_id in courses:
            instructors = []
            course = courses[course_id]
            for user_id in course[DBc.FIELD_COURSE_INSTRUCTOR]:
                user = self.users.get(user_id)
                if user:
                    name = user[DBc.FIELD_USER_NAME] or user[DBc.FIELD_USER_USER_NAME]
                    instructors.append(name)
            course[DBc.FIELD_COURSE_INSTRUCTOR] = instructors
        if len(self.users) == 0:
            warn("USER:No users!")
        processed_data = raw_data
        # user collection needs courseIds and droppedCourseIds
        processed_data[RD_DATA][DBc.COLLECTION_USER] = self.users
        processed_data[RD_DATA][DBc.COLLECTION_COURSE] = courses
        return processed_data
