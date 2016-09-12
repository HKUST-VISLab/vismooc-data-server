<<<<<<< HEAD
import json
import pymongo
from mathematician.DB.mongo_dbhelper import MongoDB


def db_creater(db_configfile="./dbconfig.json"):
    with open(db_configfile, 'r') as db_config:
        config = json.load(db_config)
    
    db = MongoDB("localhost", config['db_name'])
    db.clear()

    # There is some trouble here
    # db.add_user(config['db_user'], config['db_passwd'])
    for collection in config['db_collections']:
        
        #first construct the validator
        validation_list = [] 
        if collection["fields"]:
            for a_field in collection["fields"]:
                validation_list.append({a_field["field_name"] : a_field["validation"]})
        
        #create collection with specified validation option
        a_collection = db.create_collection(collection['collection_name'], validator={"$and":validation_list})
            

        #second make the index
        if collection["index"]:
            indexes = []
            for a_index in collection["index"]:
                tmp_index = (a_index["field"], a_index["order"])
                indexes.append(tmp_index)
            a_collection.create_index(indexes)
                

if __name__ == '__main__':
    db_creater()
    print("Successfully created the database")
=======
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
>>>>>>> dev
