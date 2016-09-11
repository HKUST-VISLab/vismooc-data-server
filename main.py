import json
import time
from os import listdir, path

from mathematician.Processor import FormatLogFile, FormatUserFile, FormatVideoFile
from mathematician.pipe import Pipe


def main():
    print('let test')
    # log_file_dir = "./test-data/log-files"
    # files = [path.join(log_file_dir, f) for f in listdir(log_file_dir) if path.isfile(
    #     path.join(log_file_dir, f))]
    # for file_name in files:
    #     start_time = time.time()
    #     with open(file_name, "r") as file:
    #         raw_data = file.readlines()
    #         pipeline = Pipe()
    #         logfile_processor = FormatLogFile()
    #         clean_logfile = pipeline.input(
    #             raw_data).pipe(logfile_processor).output()
    #         write_file = open(file_name + ".clean", "w")
    #         write_file.write(json.dumps(clean_logfile["data"]))
    #         write_file.close()

    #         print(str(path.getsize(file_name)) +
    #               " spend:" + str(time.time() - start_time))

    #     break

    course_info_file_dir = './test-data/course-info-files/hkustx-2014-08-19'
    files = [path.join(course_info_file_dir, f) for f in listdir(
        course_info_file_dir) if path.isfile(path.join(course_info_file_dir, f))]
    for file_name in files:
        start_time = time.time()
        if '-auth_user-' in file_name:
            print(file_name)
            with open(file_name, 'r') as file:
                raw_data = file.readlines()
                pipeline = Pipe()
                userfile_processor = FormatUserFile()
                clean_userfile = pipeline.input(
                    raw_data).pipe(userfile_processor).output()
                write_file = open(file_name + ".clean", "w")
                write_file.write(json.dumps(clean_userfile["data"]))
                write_file.close()

                print(str(path.getsize(file_name)) +
                      " spend:" + str(time.time() - start_time))
        elif '-course_structure-' in file_name:
            print(file_name)
            with open(file_name, 'r') as file:
                raw_data = file.readlines()
                pipeline = Pipe()
                videofile_processor = FormatVideoFile()
                clean_userfile = pipeline.input(
                    raw_data).pipe(videofile_processor).output()
                write_file = open(file_name + ".clean", "w")
                write_file.write(json.dumps(clean_userfile["data"]))
                write_file.close()

                print(str(path.getsize(file_name)) +
                      " spend:" + str(time.time() - start_time))

if __name__ == "__main__":
    main()
