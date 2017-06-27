'''This processor can process the data in MoocDB then insert them into mongodb
   for vismooc.
'''
from .course import CourseProcessor
from .db import DBProcessor
from .enrollment import EnrollmentProcessor
from .forum import ForumProcessor
from .grade import GradeProcessor
from .log import LogProcessor
from .user import UserProcessor
from .video import VideoProcessor

__all__ = ['course', 'db', 'enrollment', 'forum', 'grade', 'log', 'user', 'video']
