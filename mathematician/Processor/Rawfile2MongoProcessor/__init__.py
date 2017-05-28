'''This processor can process the raw data files then insert them into mongodb
   for vismooc.
'''
from .CourseProcessor import CourseProcessor
from .CourseRoleProcessor import CourseRoleProcessor
from .DBProcessor import DBProcessor
from .EnrollmentProcessor import EnrollmentProcessor
from .ForumProcessor import ForumProcessor
from .GradeProcessor import GradeProcessor
from .LogProcessor import LogProcessor
from .UserProcessor import UserProcessor
from .MetaDBProcessor import MetaDBProcessor

__all__ = ['CourseProcessor', 'CourseRoleProcessor', 'DBProcessor',
           'EnrollmentProcessor', 'ForumProcessor', 'GradeProcessor',
           'LogProcessor', 'UserProcessor', 'MetaDBProcessor']
