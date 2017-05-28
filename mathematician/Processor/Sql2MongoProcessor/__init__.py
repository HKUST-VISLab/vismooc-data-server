'''This processor can process the data in MoocDB then insert them into mongodb
   for vismooc.
'''
from .CourseProcessor import ProcessCourseTable as CourseProcessor
from .DBProcessor import DumpToDB as DBProcessor
from .EnrollmentProcessor import ProcessEnrollmentTable as EnrollmentProcessor
from .ForumProcessor import ProcessForumTable as ForumProcessor
from .GradeProcessor import ProcessGradeTable as GradeProcessor
from .LogProcessor import ProcessLogTable as LogProcessor
from .UserProcessor import ProcessUserTable as UserProcessor
from .VideoProcessor import ProcessVideoTable as VideoProcessor

__all__ = ['CourseProcessor', 'DBProcessor', 'EnrollmentProcessor', \
    'ForumProcessor', 'GradeProcessor', 'LogProcessor', \
    'UserProcessor', 'VideoProcessor']

