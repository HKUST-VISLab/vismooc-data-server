# -*- coding: utf-8 -*-
import json
import time
from os import listdir, path

from mathematician.Processor import FormatLogFile, FormatUserFile, FormatCourseStructFile
from mathematician.pipe import PipeLine


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
    pipeline = PipeLine()
    pipeline.input_files(files).pipe(
        FormatUserFile()).pipe(FormatCourseStructFile())

    start_time = time.time()
    processed_data = pipeline.output()
    write_file = open("processed_data.json", "w")
    write_file.write(json.dumps(processed_data))
    write_file.close()
    print("spend:" + str(time.time() - start_time))


if __name__ == "__main__":
    main()
    # filename = './test-data/course-info-files/hkustx-2014-08-19/HKUSTx-COMP102x-2T2014-auth_userprofile-prod-analytics.sql'
    # with open(filename, 'r', encoding='utf-8') as file:
    #     text = file.readlines()
    #     for i in range(20):
    #         print(str(i) + ":" + text[i])
