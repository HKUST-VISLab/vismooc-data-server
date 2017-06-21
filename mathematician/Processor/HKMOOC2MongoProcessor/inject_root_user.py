from mathematician.config import DBConfig as DBc
from mathematician.logger import info
from mathematician.pipe import PipeModule

from .constant import RD_DATA


class InjectSuperUserProcessor(PipeModule):
    '''Inject super user to access all courses
    '''
    order = 5

    def __init__(self, username=None):
        super().__init__()
        self.super_user = set(username) if isinstance(username, list) else {username}

    def process(self, raw_data, raw_data_filenames=None):
        '''process the data
        '''
        info("Inject Super User:" + str(self.super_user))
        users = raw_data[RD_DATA][DBc.COLLECTION_USER]
        target_users = []
        for user_id in users:
            user = users[user_id]
            username = user[DBc.FIELD_USER_USER_NAME]
            if username in self.super_user:
                target_users.append(user)

        courses = raw_data[RD_DATA][DBc.COLLECTION_COURSE]
        for user in target_users:
            for course_id in courses:
                user[DBc.FIELD_USER_COURSE_ROLE][course_id] = ["instructor", "staff"]

        processed_data = raw_data
        processed_data[RD_DATA][DBc.COLLECTION_USER] = users
        return processed_data
