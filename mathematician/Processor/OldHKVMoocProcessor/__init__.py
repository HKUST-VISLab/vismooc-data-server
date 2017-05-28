from .CourseProcessor import ProcessCourseStructureFile as CourseProcessor
from .CourseRoleProcessor import FormatCourseRoleFile as CourseRoleProcessor
from .DBProcessor import DumpToDB as DBProcessor
from .EnrollmentProcessor import FormatEnrollmentFile as EnrollmentProcessor
from .ForumProcessor import FormatForumFile as ForumProcessor
from .GradeProcessor import FormatGradeFile as GradeProcessor
from .LogProcessor import FormatLogFile as LogProcessor
from .UserProcessor import FormatUserFile as UserProcessor
from .MetaDBProcessor import ProcessMetadbFiles as MetaDBProcessor

__all__ = ['CourseProcessor', 'CourseRoleProcessor', 'DBProcessor', \
    'EnrollmentProcessor', 'ForumProcessor', 'GradeProcessor', \
    'LogProcessor', 'UserProcessor', 'MetaDBProcessor']

