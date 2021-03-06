'''The entry point to start the rawfile2mongo processor
'''
from datetime import datetime
from os import listdir, path

from mathematician.pipe import PipeLine
from mathematician.Processor.Rawfile2MongoProcessor import (course,
                                                            course_role,
                                                            db,
                                                            enrollment,
                                                            forum,
                                                            grade,
                                                            log,
                                                            user,
                                                            meta_info)

DB_NAME = 'testVismoocElearning'
DB_HOST = 'localhost'


def get_files(dir_name):
    files = []
    if '.ignore' in dir_name:
        return []
    for file_name in listdir(dir_name):
        if path.isfile(path.join(dir_name, file_name)):
            files.append(path.join(dir_name, file_name))
        elif path.isdir(path.join(dir_name, file_name)):
            files.extend(get_files(path.join(dir_name, file_name)))
    return files

def main():
    '''The main function
    '''
    courses_dir = '/vismooc-test-data/elearning-data/'
    filenames = get_files(courses_dir)

    pipeline = PipeLine()
    pipeline.input_files(filenames) \
        .pipe(meta_info.MetaDBProcessor()) \
        .pipe(course.CourseProcessor()) \
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
