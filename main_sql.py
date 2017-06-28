'''The entry point to start the sql2mongo processor
'''
import sys
from datetime import datetime

from mathematician.pipe import PipeLine
import mathematician.config as config
from mathematician.Processor.Sql2MongoProcessor import (DBProcessor,
                                                        CourseProcessor,
                                                        EnrollmentProcessor,
                                                        ForumProcessor,
                                                        GradeProcessor,
                                                        LogProcessor,
                                                        UserProcessor,
                                                        VideoProcessor)


def main():
    '''The entry function
    '''
    if len(sys.argv) >= 2:
        config.init_config(sys.argv[1])
    else:
        config.init_config('./config.json')
    pipeline = PipeLine()
    pipeline.input_files([]).pipe(CourseProcessor()).pipe(VideoProcessor()).pipe(
        EnrollmentProcessor()).pipe(LogProcessor()).pipe(UserProcessor()).pipe(
            ForumProcessor()).pipe(GradeProcessor()).pipe(DBProcessor())
    start_time = datetime.now()
    pipeline.execute()
    print('spend time:' + str(datetime.now() - start_time))

if __name__ == "__main__":
    main()
