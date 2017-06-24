'''The entry point to start the sql2mongo processor
'''
from datetime import datetime

from mathematician.pipe import PipeLine
from mathematician.Processor.Sql2MongoProcessor import (DumpToDB,
                                                        ProcessCourseTable,
                                                        ProcessEnrollmentTable,
                                                        ProcessForumTable,
                                                        ProcessGradeTable,
                                                        ProcessLogTable,
                                                        ProcessUserTable,
                                                        ProcessVideoTable)

def main():
    '''The entry function
    '''
    pipeline = PipeLine()
    pipeline.input_files([]).pipe(ProcessCourseTable()).pipe(ProcessVideoTable()).pipe(
        ProcessEnrollmentTable()).pipe(ProcessLogTable()).pipe(ProcessUserTable()).pipe(
            ProcessForumTable()).pipe(ProcessGradeTable()).pipe(DumpToDB())
    start_time = datetime.now()
    pipeline.execute()
    print('spend time:' + str(datetime.now() - start_time))

if __name__ == "__main__":
    main()
