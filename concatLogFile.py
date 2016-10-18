import re
from os import listdir
from os.path import isfile, join
from datetime import datetime

def process(raw_data):
    data_to_be_processed = raw_data
    if data_to_be_processed is None:
        return raw_data

    pattern_wrong_username = r'"username"\s*:\s*"",'
    pattern_right_eventsource = r'"event_source"\s*:\s*"browser"'
    pattern_right_eventtype = r'"event_type"\s*:\s*"(hide_transcript|load_video|' + \
        r'pause_video|play_video|seek_video|show_transcript|speed_change_video|stop_video|' + \
        r'video_hide_cc_menu|video_show_cc_menu)"'

    pattern_context = r',?\s*("context"\s*:\s*{[^}]*})'
    pattern_event = r',?\s*("event"\s*:\s*"([^"]|\\")*(?<!\\)")'
    pattern_username = r',?\s*("username"\s*:\s*"[^"]*")'
    pattern_time = r',?\s*("time"\s*:\s*"[^"]*")'
    pattern_event_json_escape_ = r'"(?={)|"$'

    re_wrong_username = re.compile(pattern_wrong_username)
    re_right_eventsource = re.compile(pattern_right_eventsource)
    re_right_eventtype = re.compile(pattern_right_eventtype)
    re_context = re.compile(pattern_context)
    re_event = re.compile(pattern_event)
    re_username = re.compile(pattern_username)
    re_time = re.compile(pattern_time)
    re_event_json_escape = re.compile(pattern_event_json_escape_)

    events = []
    for line in data_to_be_processed:
        event_type = re_right_eventtype.search(line)
        if re_wrong_username.search(line) is None and \
            re_right_eventsource.search(line) is not None and \
            event_type is not None:
            events.append(line)

    return events

if __name__ == "__main__":

    # test_file_dir = './test-data/course-info-files/hkustx-2014-08-19'
    # file_names = [join(test_file_dir, f) for f in listdir(
    #     test_file_dir) if isfile(join(test_file_dir, f))]

    # file_names.append('./test-data/log-files/hkustx-edx-events-2014-06-18.log')
    # pipeline = PipeLine()
    # pipeline.input_files(file_names).pipe(FormatCourseStructFile()).pipe(
    #     FormatEnrollmentFile()).pipe(FormatLogFile()).pipe(FormatUserFile()).pipe(DumpToDB())

    # start_time = datetime.now()
    # pipeline.excute()
    # print('spend time:' + str(datetime.now() - start_time))
    course_file_dir = './test-data/log-files/'
    file_names = [join(course_file_dir, f) for f in listdir(course_file_dir) if isfile(join(course_file_dir, f))]
    for file_name in file_names:
        if '-events-' in file_name:
            print("Processing " + file_name)
            with open(file_name, 'r') as file:
                raw_data = file.readlines()
                result = process(raw_data)
                with open("./test-data/hkustx-events-log-total.log", 'a') as f:
                    f.write("".join(result))
                    f.flush()
    print("Done")
