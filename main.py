from os import listdir
from os import path
import time
from mathematician.formatLogFile import ProcessLogFile
from mathematician.pipe import Pipe


def main():
    print('let test')
    logFileDir = "./test-data"
    files = [path.join(logFileDir, f) for f in listdir(logFileDir) if path.isfile(
        path.join(logFileDir, f))]
    for file_name in files:
        start_time = time.time()
        with open(file_name, "r") as file:
            raw_data = file.readlines()
            pipeline = Pipe()
            logfile_processor = ProcessLogFile()
            clean_logfile = pipeline.input(
                raw_data).pipe(logfile_processor).output()
            write_file = open(file_name + ".clean", "w")
            write_file.write("".join(clean_logfile))
            write_file.close()

            print(str(path.getsize(file_name)) + " spend:" + str(time.time() - start_time))

    """        
    test_string = [r'{""username"": ""HarvengtD"", ""event_type"": ""seq_next"", ""ip"": ""80.201.125.32"", ""agent"": ""Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53"", ""host"": ""courses.edx.org"", ""session"": ""b318d9d5379e3bbe8f8ce60965f6d94b"", ""event"": ""{\""old\"":3,\""new\"":4,\""id\"":\""i4x://HKUSTx/COMP102x/sequential/0a013f1e2ed64785b324994d67a27513\""}"", ""event_source"": ""browser"", ""context"": {""user_id"": 3483023, ""org_id"": ""HKUSTx"", ""course_id"": ""HKUSTx/COMP102x/2T2014"", ""path"": ""/event""}, ""time"": ""2014-09-02T19:52:52.670293+00:00"", ""page"": ""https://courses.edx.org/courses/HKUSTx/COMP102x/2T2014/courseware/1020d90b174142239fcdefc2f8555d55/0a013f1e2ed64785b324994d67a27513/""}']
    """

if __name__ == "__main__":
    main()
