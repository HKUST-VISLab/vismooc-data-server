'''The entry point to start the sql2mongo processor
'''
import sys
from datetime import datetime

from mathematician.pipe import PipeLine
import mathematician.config as config
from mathematician.Processor.Sql2MongoProcessor import (course,
                                                        db,
                                                        enrollment,
                                                        forum,
                                                        grade,
                                                        log,
                                                        user,
                                                        video)


def main():
    '''The entry function
    '''
    if len(sys.argv) >= 2:
        config.init_config(sys.argv[1])
    else:
        config.init_config('./config.json')
    pipeline = PipeLine()
    pipeline.input_files([]) \
        .pipe(course.CourseProcessor()) \
        .pipe(video.VideoProcessor()) \
        .pipe(enrollment.EnrollmentProcessor()) \
        .pipe(log.LogProcessor()) \
        .pipe(user.UserProcessor()) \
        .pipe(forum.ForumProcessor()) \
        .pipe(grade.GradeProcessor()) \
        .pipe(db.DBProcessor())
    start_time = datetime.now()
    pipeline.execute()
    print('spend time:' + str(datetime.now() - start_time))

if __name__ == "__main__":
    main()
