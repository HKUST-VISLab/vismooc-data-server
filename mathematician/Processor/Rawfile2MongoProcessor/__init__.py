'''This processor can process the raw data files then insert them into mongodb
   for vismooc.
'''
from .course import CourseProcessor
from .course_role import CourseRoleProcessor
from .db import DBProcessor
from .enrollment import EnrollmentProcessor
from .forum import ForumProcessor
from .grade import GradeProcessor
from .log import LogProcessor
from .user import UserProcessor
from .meta_info import MetaDBProcessor

__all__ = ['course', 'course_role', 'db', 'enrollment', 'forum', 'grade', 'log', 'user', 'meta_info']
