'''The entry point to start the sql2mongo processor
'''
from datetime import datetime

from mathematician.pipe import PipeLine
from mathematician.Processor.Sql2MongoProcessor import (DBProcessor,
                                                        CourseProcessor,
                                                        EnrollmentProcessor,
                                                        ForumProcessor,
                                                        GradeProcessor,
                                                        LogProcessor,
                                                        UserProcessor,
                                                        VideoProcessor)
import mathematician.config as config 

def main():
    '''The entry function
    '''
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
