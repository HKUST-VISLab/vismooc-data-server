import re
import json
import queue
import struct
import io
from datetime import timedelta, datetime
from operator import itemgetter
import mathematician.httphelper as httphelper
from ..pipe import PipeModule
from ..DB.mongo_dbhelper import MongoDB
from ..config import DBConfig as DBc, ThirdPartyKeys, FilenameConfig

re_ISO_8601_duration = re.compile(
    r"^(?P<sign>[+-])?"
    r"P(?!\b)"
    r"(?P<years>[0-9]+([,.][0-9]+)?Y)?"
    r"(?P<months>[0-9]+([,.][0-9]+)?M)?"
    r"(?P<weeks>[0-9]+([,.][0-9]+)?W)?"
    r"(?P<days>[0-9]+([,.][0-9]+)?D)?"
    r"((?P<separator>T)(?P<hours>[0-9]+([,.][0-9]+)?H)?"
    r"(?P<minutes>[0-9]+([,.][0-9]+)?M)?"
    r"(?P<seconds>[0-9]+([,.][0-9]+)?S)?)?$"
)

def parse_duration(datestring):
    if not isinstance(datestring, str):
        raise TypeError("Expecting a string %r" % datestring)
    match = re_ISO_8601_duration.match(datestring)
    if not match:
        raise BaseException("Unable to parse duration string %r" % datestring)
    groups = match.groupdict()
    for key, val in groups.items():
        if key not in ('separator', 'sign'):
            if val is None:
                groups[key] = "0n"
            groups[key] = float(groups[key][:-1].replace(',', '.'))
    if groups["years"] == 0 and groups["months"] == 0:
        ret = timedelta(days=groups["days"], hours=groups["hours"],
                        minutes=groups["minutes"], seconds=groups["seconds"],
                        weeks=groups["weeks"])
        if groups["sign"] == '-':
            ret = timedelta(0) - ret
    else:
        raise BaseException("there must be something woring in this time string")
    return ret

def split(text, separator=','):
    """split a text with `separtor`"""
    
    tmp_stack = []
    results = []
    quota_number = 0
    for i, letter in enumerate(text):
        if letter == "'" and text[i-1] != "\\":
            quota_number += 1
        elif letter == separator and (quota_number == 2 or quota_number == 0):
            results.append("".join(tmp_stack))
            tmp_stack = []
            quota_number = 0
        else:
            tmp_stack.append(letter)

        if i == len(text)-1:
            results.append("".join(tmp_stack))
            return results

class FormatCourseStructFile(PipeModule):

    order = 1

    def __init__(self):
        super().__init__()
        self.course_video_videokey = {}
        self.course_video_coursekey = {}
        self.video_duration = {}
        self.course_instructor = {}
        self.video_url_duration = {}
        self.video_id_info = {}
        self.YOUTUBE_URL_PREFIX = 'https://www.youtube.com/watch?v='

        youtube_api_host = 'https://www.googleapis.com/youtube/v3/videos'
        params = 'part=contentDetails&key=' + ThirdPartyKeys.Youtube_key

        self.youtube_api = youtube_api_host + '?' + params

    def load_data(self, raw_data):
        '''
        Load target file
        '''
        self.course_overview = raw_data.get('course_overviews_courseoverview')
        self.edx_videos = raw_data.get('edxval_video')
        self.edx_course_videos = raw_data.get('edxval_coursevideo')
        self.video_encode = raw_data.get('edxval_encodedvideo')
        self.course_access_role = raw_data.get('student_courseaccessrole')
        self.course_structures = raw_data.get('course_in_mongo')
        

    def get_video_url_duration_from_sql(self):
        video_id_url = {}
        for video_encode_item in self.video_encode:
            video_encode_items = split(video_encode_item)
            video_id = video_encode_items[7]
            video_url = video_encode_items[3]
            if "http" not in video_url:
                video_url = self.YOUTUBE_URL_PREFIX + video_url
            video_id_url[video_id] = video_url
        for video_item in self.edx_videos:
            record = split(video_item)
            if video_id_url.get(record[0]):
                self.video_url_duration[record[4]] = video_id_url[record[0]]

    def parse_video_duration(self, url):
        header = {"Range":"bytes=0-100"}
        result = httphelper.get(url, header)
        if result.get_return_code() < 200 or result.get_return_code() >= 300:
            return -1
        bio = io.BytesIO(result.get_content())
        data = bio.read(8)
        al, an = struct.unpack('>I4s', data)
        an = an.decode()
        assert an == 'ftyp'
        bio.read(al-8)
        data = bio.read(8)
        al, an = struct.unpack('>I4s', data)
        an = an.decode()
        assert an == 'moov'
        data = bio.read(8)
        al, an = struct.unpack('>I4s', data)
        an = an.decode()
        assert an == 'mvhd'
        data = bio.read(20)
        infos = struct.unpack('>12x2I', data)
        video_length = int(infos[1]) // int(infos[0])
        return video_length

    def process(self, raw_data, raw_data_filenames=None):
        print("Processing FormatCourseStructFile")
        COURSE_YEAR_NAME = 'course_year'
        pattern_time = "%Y-%m-%d %H:%M:%S.%f"
        course_year_pattern = r'^course-[\w|:|\+]+(?P<' + COURSE_YEAR_NAME + r'>[0-9]{4})\w*'
        self.load_data(raw_data)
        db = MongoDB(DBc.DB_HOST, DBc.DB_NAME)
        video_collection = db.get_collection(DBc.COLLECTION_VIDEO)
        # the db file stay unchanged
        if self.course_overview is None:
            videos = {video[DBc.FIELD_VIDEO_ORIGINAL_ID] : video for \
                video in video_collection.find({})}
            processed_data = raw_data
            processed_data['data'][DBc.COLLECTION_VIDEO] = videos
            return processed_data

        for row in self.edx_course_videos:
            records = split(row)
            course_id = records[1]
            course_id = course_id[course_id.index(':')+1:]
            video_id = records[2]
            self.course_video_videokey[video_id] = course_id
            self.course_video_coursekey.setdefault(course_id, []).append(video_id)

        for one_access_role in self.course_access_role:
            records = split(one_access_role)
            if records[3] != 'instructor':
                continue
            course_id = records[2]
            course_id = course_id[course_id.index(':')+1:]
            course_one_instructor = records[4]
            self.course_instructor.setdefault(course_id, []).append(course_one_instructor)

        re_course_year = re.compile(course_year_pattern)
        videos = {}
        courses = {}
        tmp_youtube_video_dict = {}
        tmp_other_video_dict = {}
        # for video_item in self.edx_videos:
        #     video = {}
        #     video_records = split(video_item)
        #     video_original_id = video_records[0]
        #     create_date = datetime.strptime(video_records[1], pattern_time) \
        #         if video_records[1] else None
        #     video[DBc.FIELD_VIDEO_ORIGINAL_ID] = video_original_id
        #     video[DBc.FIELD_VIDEO_NAME] = video_records[2]
        #     video[DBc.FIELD_VIDEO_SECTION] = video_records[3]
        #     video[DBc.FIELD_VIDEO_DESCRIPTION] = video_records[3]
        #     video[DBc.FIELD_VIDEO_RELEASE_DATE] = create_date and create_date.timestamp()
        #     video[DBc.FIELD_VIDEO_DURATION] = video_records[4]
        #     video[DBc.FIELD_VIDEO_METAINFO] = None
        #     video[DBc.FIELD_VIDEO_COURSE_ID] = self.course_video_videokey[video_original_id]
        #     video[DBc.FIELD_VIDEO_URL] = self.video_url_dict.get(video_original_id)
        #     videos[video_original_id] = video
        for course_item in self.course_overview:
            course = {}
            course_records = split(course_item)
            # if the course info already exist, then skip it
            if course.get(course_records[3]):
                continue
            course_start_time = datetime.strptime(
                course_records[8], pattern_time) if course_records[8] != "NULL" else None
            course_end_time = datetime.strptime(
                course_records[9], pattern_time) if course_records[9] != "NULL" else None
            advertised_start_time = datetime.strptime(
                course_records[10], pattern_time) if course_records[10] != "NULL" else None
            enrollment_start_time = datetime.strptime(
                course_records[26], pattern_time) if course_records[26] != "NULL" else None
            enrollment_end_time = datetime.strptime(
                course_records[27], pattern_time) if course_records[27] != "NULL" else None
            course_original_id = course_records[3]
            course_original_id = course_original_id[course_original_id.index(':')+1:]
            course[DBc.FIELD_COURSE_ORIGINAL_ID] = course_original_id
            course[DBc.FIELD_COURSE_NAME] = course_records[5]
            course_year_match = re_course_year.search(course_original_id)
            course[DBc.FIELD_COURSE_YEAR] = course_year_match and \
                course_year_match.group(COURSE_YEAR_NAME)
            course[DBc.FIELD_COURSE_INSTRUCTOR] = self.course_instructor[course_original_id]
            # TO DO
            course[DBc.FIELD_COURSE_STATUS] = None
            course[DBc.FIELD_COURSE_URL] = None
            course[DBc.FIELD_COURSE_IMAGE_URL] = course_records[11]
            course[DBc.FIELD_COURSE_DESCRIPTION] = course_records[35]
            course[DBc.FIELD_COURSE_STARTTIME] = course_start_time and course_start_time.timestamp()
            course[DBc.FIELD_COURSE_ENDTIME] = course_end_time and course_end_time.timestamp()
            course[DBc.FIELD_COURSE_ENROLLMENT_START] = enrollment_start_time \
                and enrollment_start_time.timestamp()
            course[DBc.FIELD_COURSE_ENROLLMENT_END] = enrollment_end_time \
                and enrollment_end_time.timestamp()
            course[DBc.FIELD_COURSE_STUDENT_IDS] = set()
            #course[DBc.FIELD_COURSE_VIDEO_IDS] = self.course_video_coursekey.get(course_original_id)
            course[DBc.FIELD_COURSE_METAINFO] = None
            course[DBc.FIELD_COURSE_ORG] = course_records[36]
            course[DBc.FIELD_COURSE_ADVERTISED_START] = advertised_start_time \
                and advertised_start_time.timestamp()
            course[DBc.FIELD_COURSE_LOWEST_PASSING_GRADE] = course_records[21]
            course[DBc.FIELD_COURSE_MOBILE_AVAILABLE] = course_records[23]
            course[DBc.FIELD_COURSE_DISPLAY_NUMBER_WITH_DEFAULT] = course_records[6]

            course_structure = self.course_structures.get(course_original_id)
            if course_structure:
                for block in course_structure['blocks']:
                    if block['block_type'] == 'course':
                        for chapter in block['fields']['children']:
                            for sequential in chapter['fields']['children']:
                                for vertical in sequential['fields']['children']:
                                    for leaf in vertical['fields']['children']:
                                        if leaf['block_type'] == 'video':
                                            if leaf['fields']:
                                                fields = leaf['fields']
                                                video_original_id = leaf['block_id']
                                                videos[video_original_id] = {}
                                                videos[video_original_id][DBc.FIELD_VIDEO_ORIGINAL_ID] = video_original_id
                                                videos[video_original_id][DBc.FIELD_VIDEO_NAME] = fields.get('display_name')
                                                videos[video_original_id][DBc.FIELD_VIDEO_URL] = (fields.get('youtube_id_1_0') \
                                                    and self.YOUTUBE_URL_PREFIX + fields.get('youtube_id_1_0')) or (fields.get('html5_sources') \
                                                    and fields.get('html5_sources')[0])
                                                videos[video_original_id][DBc.FIELD_VIDEO_TEMPORAL_HOTNESS] = {}
                                                # TO DO
                                                videos[video_original_id][DBc.FIELD_VIDEO_DURATION] = self.video_url_duration.get( \
                                                    videos[video_original_id][DBc.FIELD_VIDEO_URL])
                                                videos[video_original_id][DBc.FIELD_VIDEO_DESCRIPTION] = fields.get('display_name')
                                                videos[video_original_id][DBc.FIELD_VIDEO_SECTION] = chapter['fields']['display_name'] + ', ' + \
                                                    sequential['fields']['display_name'] + ', ' + vertical['fields']['display_name']
                                                videos[video_original_id][DBc.FIELD_VIDEO_COURSE_ID] = course_original_id

                                                if videos[video_original_id][DBc.FIELD_VIDEO_URL] and 'youtube' in videos[video_original_id][DBc.FIELD_VIDEO_URL]:
                                                    url = videos[video_original_id][DBc.FIELD_VIDEO_URL]
                                                    youtube_id = url[url.index('v=')+2:]
                                                    tmp_youtube_video_dict[youtube_id] = video_original_id
                                                elif videos[video_original_id][DBc.FIELD_VIDEO_URL]:
                                                    tmp_other_video_dict.setdefault(videos[video_original_id][DBc.FIELD_VIDEO_URL], []).append(video_original_id)

                                                course.setdefault(DBc.FIELD_COURSE_VIDEO_IDS, []).append(video_original_id)

            courses[course_original_id] = course

        # fetch the video duration from youtube_api_v3
        urls = [self.youtube_api + '&id=' +
                youtube_id for youtube_id in tmp_youtube_video_dict.keys()]
        broken_youtube_id = set([youtube_id for youtube_id in tmp_youtube_video_dict.keys()])
        results = httphelper.get_list(urls, limit=60)
        for result in results:
            result = json.loads(str(result, 'utf-8'))
            if len(result["items"]) == 0:
                continue
            video_id = result["items"][0]["id"]
            broken_youtube_id.discard(video_id)
            duration = parse_duration(result["items"][0]["contentDetails"]["duration"])
            videos[tmp_youtube_video_dict[video_id]][DBc.FIELD_VIDEO_DURATION] = int(duration.total_seconds())
        
        for url in tmp_other_video_dict.keys():
            video_duration = self.parse_video_duration(url)
            # if url == "https://www.hkmooc.hk/COMP1022P/videos/Part%202/w4-l9/COMP102.2x-L9-T1.4-Recursive_Method_Calls.mp4":
            #     print('find this url ' + str(video_duration))
            for videoID in tmp_other_video_dict[url]:
                videos[videoID][DBc.FIELD_VIDEO_DURATION] = video_duration

        processed_data = raw_data
        processed_data['data'][DBc.COLLECTION_VIDEO] = videos
        processed_data['data'][DBc.COLLECTION_COURSE] = courses
        return processed_data


class FormatUserFile(PipeModule):

    order = 2

    def __init__(self):
        super().__init__()
        self._userprofile = {}

    def load_data(self, raw_data):
        '''
        Load target file
        '''
        self.raw_user_profile = raw_data.get('auth_userprofile')
        self.user_info = raw_data.get('auth_user')

    def process(self, raw_data, raw_data_filenames=None):
        print("Processing FormatUserFile")
        self.load_data(raw_data)
        if self.raw_user_profile is not None:
            for record in self.raw_user_profile:
                fields = split(record)
                self._userprofile[fields[16]] = fields
        if self.user_info is None:
            return raw_data
        users = {}
        for record in self.user_info:
            user_fields = split(record)
            user = {}
            user_id = user_fields[0]
            # print(user_id)
            user_profile = self._userprofile.get(user_id)
            birth_year = datetime.strptime(user_profile[6], '%Y')\
                 if (user_profile and (user_profile[6] != "NULL" and \
                 len(user_profile[6]) == 4)) else None
            user[DBc.FIELD_USER_USER_NAME] = user_fields[4]
            user[DBc.FIELD_USER_LANGUAGE] = user_profile and user_profile[4]
            user[DBc.FIELD_USER_LOCATION] = user_profile and user_profile[5]
            user[DBc.FIELD_USER_BIRTH_DATE] = birth_year and birth_year.timestamp()
            user[DBc.FIELD_USER_EDUCATION_LEVEL] = user_profile and user_profile[8]
            user[DBc.FIELD_USER_GENDER] = user_profile and user_profile[7]
            user[DBc.FIELD_USER_COURSE_IDS] = set()
            user[DBc.FIELD_USER_DROPPED_COURSE_IDS] = set()
            user[DBc.FIELD_USER_BIO] = user_profile and user_profile[14]
            user[DBc.FIELD_USER_COUNTRY] = user_profile and (user_profile[11] or user_profile[5])
            user[DBc.FIELD_USER_NAME] = user_fields[5] + user_fields[6]
            user[DBc.FIELD_USER_ORIGINAL_ID] = user_id
            users[user[DBc.FIELD_USER_ORIGINAL_ID]] = user

        processed_data = raw_data
        # user collection needs courseIds and droppedCourseIds
        processed_data['data'][DBc.COLLECTION_USER] = users

        return processed_data


class FormatEnrollmentFile(PipeModule):

    order = 3
    ENROLL = "enroll"
    UNENROLL = "unenroll"
    action = {'1': ENROLL, '0': UNENROLL}

    def __init__(self):
        super().__init__()
        self.course_enrollment = None

    def load_data(self, raw_data):
        '''
        Load target file
        '''
        self.course_enrollment = raw_data.get('student_courseenrollment')

    def process(self, raw_data, raw_data_filenames=None):
        print("Processing FormatEnrollmentFile")
        self.load_data(raw_data)
        if self.course_enrollment is None:
            return raw_data
        pattern_time = "%Y-%m-%d %H:%M:%S.%f"
        courses = raw_data['data'][DBc.COLLECTION_COURSE]
        users = raw_data['data'][DBc.COLLECTION_USER]

        enrollments = []
        for enroll_item in self.course_enrollment:
            enrollment = {}
            records = split(enroll_item)
            user_id = records[5]
            course_id = records[1]
            course_id = course_id[course_id.index(':')+1:]            
            enrollment_time = datetime.strptime(records[2], pattern_time) \
                if records[2] != "NULL" else None
            enrollment[DBc.FIELD_ENROLLMENT_USER_ID] = user_id
            enrollment[DBc.FIELD_ENROLLMENT_COURSE_ID] = course_id
            enrollment[DBc.FIELD_ENROLLMENT_TIMESTAMP] = enrollment_time \
                and enrollment_time.timestamp()
            enrollment[DBc.FIELD_ENROLLMENT_ACTION] = FormatEnrollmentFile.action.get(records[3])
            enrollments.append(enrollment)
            # fill in user collection
            if enrollment[DBc.FIELD_ENROLLMENT_ACTION] == FormatEnrollmentFile.ENROLL:
                users[user_id][DBc.FIELD_USER_COURSE_IDS].add(course_id)
                users[user_id][DBc.FIELD_USER_DROPPED_COURSE_IDS].discard(course_id)
                courses[course_id][DBc.FIELD_COURSE_STUDENT_IDS].add(user_id)
            elif enrollment[DBc.FIELD_ENROLLMENT_ACTION] == FormatEnrollmentFile.UNENROLL:
                users[user_id][DBc.FIELD_USER_DROPPED_COURSE_IDS].add(course_id)
                users[user_id][DBc.FIELD_USER_COURSE_IDS].discard(course_id)
                courses[course_id][DBc.FIELD_COURSE_STUDENT_IDS].discard(user_id)

        processed_data = raw_data
        # course and users collection are completed
        processed_data['data'][DBc.COLLECTION_ENROLLMENT] = enrollments
        # processed_data['data'][DBc.COLLECTION_USER] = list(users.values())
        # processed_data['data'][DBc.COLLECTION_COURSE] = list(courses.value())
        return processed_data

class FormatLogFile(PipeModule):

    order = 4

    def __init__(self):
        super().__init__()

    def load_data(self, data_filenames):
        '''
        Load target file
        '''
        for filename in data_filenames:
            if FilenameConfig.Clickstream_suffix in filename:
                with open(filename, 'r', encoding='utf-8') as file:
                    raw_data = file.readlines()
                    yield raw_data

    def process(self, raw_data, raw_data_filenames=None):
        print("Processing log files")
        all_data_to_be_processed = self.load_data(raw_data_filenames)

        if all_data_to_be_processed is None:
            return raw_data

        pattern_wrong_username = r'"username"\s*:\s*"",'
        pattern_right_eventsource = r'"event_source"\s*:\s*"browser"'
        pattern_right_eventtype = r'"event_type"\s*:\s*"(hide_transcript|load_video|' + \
            r'pause_video|play_video|seek_video|show_transcript|speed_change_video|stop_video|'+ \
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

        # temp_video_dict = {video[DBc.FIELD_VIDEO_ORIGINAL_ID]: video for video in raw_data[
        #     'data'][DBc.COLLECTION_VIDEO]}
        temp_video_dict = raw_data['data'][DBc.COLLECTION_VIDEO]

        events = []
        denselogs = {}
        raw_logs = []
        pattern_time = "%Y-%m-%dT%H:%M:%S.%f+00:00"
        count = 0
        for data_to_be_processed in all_data_to_be_processed:
            count += 1
            # print("This is " + str(count) +"th log file")
            for line in data_to_be_processed:
                event_type = re_right_eventtype.search(line)
                if re_wrong_username.search(line) is None and \
                re_right_eventsource.search(line) is not None and \
                event_type is not None:

                    context = re_context.search(line)
                    event_field = re_event.search(line)
                    username = re_username.search(line)
                    timestamp = re_time.search(line)
                    temp_data = [event_type.group()]
                    if context is not None:
                        temp_data.append(context.group(1))
                    if event_field is not None:
                        temp_data.append(re_event_json_escape.sub(
                            '', event_field.group(1).replace('\\', '')))
                    if username is not None:
                        temp_data.append(username.group(1))
                    if timestamp is not None:
                        temp_data.append(timestamp.group(1))
                    str_temp_data = "{" + ",".join(temp_data) + "}"
                    str_temp_data = str_temp_data.replace('.,', ',', 1)
                    temp_data = json.loads(str_temp_data)
                    event = {}
                    click = {}
                    event_context = temp_data.get('context') or {}
                    event_event = temp_data.get('event') or {}

                    video_id = event_event.get('id')
                    str_event_time = temp_data.get('time')
                    if '.' not in str_event_time:
                        str_event_time = str_event_time[:str_event_time.index("+")] + \
                            '.000000' + str_event_time[str_event_time.index("+"):]
                    course_id = event_context.get('course_id')
                    course_id = course_id[course_id.index(':')+1:]
                    event_time = datetime.strptime(str_event_time, pattern_time)
                    event[DBc.FIELD_VIDEO_LOG_USER_ID] = event_context.get('user_id')
                    event[DBc.FIELD_VIDEO_LOG_VIDEO_ID] = video_id
                    event[DBc.FIELD_VIDEO_COURSE_ID] = course_id
                    event[DBc.FIELD_VIDEO_LOG_TIMESTAMP] = event_time.timestamp()
                    event[DBc.FIELD_VIDEO_LOG_TYPE] = temp_data.get('event_type')

                    denselog_time = datetime(event_time.year, event_time.month, 
                        event_time.day).timestamp()
                    denselogs_key = (video_id + str(denselog_time)) if video_id else \
                        "none_video_id"+str(denselog_time)

                    if denselogs.get(denselogs_key) is None:
                        denselogs[denselogs_key] = {}
                        denselogs[denselogs_key][DBc.FIELD_VIDEO_DENSELOGS_COURSE_ID] = \
                            course_id
                        denselogs[denselogs_key][DBc.FIELD_VIDEO_DENSELOGS_TIMESTAMP] = \
                            denselog_time
                        denselogs[denselogs_key][DBc.FIELD_VIDEO_DENSELOGS_VIDEO_ID] = \
                            video_id
                    # TODO
                    click[DBc.FIELD_VIDEO_DENSELOGS_ORIGINAL_ID] = ''
                    click[DBc.FIELD_VIDEO_DENSELOGS_USER_ID] = event_context.get('user_id')
                    click[DBc.FIELD_VIDEO_DENSELOGS_TYPE] = temp_data.get('event_type')
                    target_attrs = {'path', 'code', 'currentTime', 'new_time', 'old_time',
                                    'new_speed', 'old_speed'}
                    event[DBc.FIELD_VIDEO_LOG_METAINFO] = {k: event_event.get(
                        k) for k in target_attrs if event_event.get(k) is not None}
                    event[DBc.FIELD_VIDEO_LOG_METAINFO]['path'] = event_context.get('path')

                    click[DBc.FIELD_VIDEO_DENSELOGS_PATH] = event_context.get('path')
                    tmp_metainfo = {k: event_event.get(
                        k) for k in target_attrs if  event_event.get(k) is not None}
                    click.update(tmp_metainfo)

                    denselogs[denselogs_key].setdefault(
                        DBc.FIELD_VIDEO_DENSELOGS_CLICKS, []).append(click)
                    date_time = str(event_time.date())
                    if temp_video_dict.get(video_id):
                        temporal_hotness = temp_video_dict[video_id][DBc.FIELD_VIDEO_TEMPORAL_HOTNESS]

                        if date_time not in temporal_hotness:
                            temporal_hotness[date_time] = 0
                        temporal_hotness[date_time] += 1
                    events.append(event)

                    line = line.replace('.,', ',', 1)
                    raw_logs.append(json.loads(line))

        processed_data = raw_data
        processed_data['data'][DBc.COLLECTION_VIDEO_LOG] = events
        processed_data['data'][DBc.COLLECTION_VIDEO_DENSELOGS] = list(denselogs.values())
        processed_data['data'][DBc.COLLECTION_VIDEO_RAWLOGS] = raw_logs
        return processed_data
class DumpToDB(PipeModule):
    order = 998

    def __init__(self):
        super().__init__()
        self.db = MongoDB(DBc.DB_HOST, DBc.DB_NAME)
        self.need_drop_collections = [DBc.COLLECTION_COURSE, DBc.COLLECTION_ENROLLMENT,\
            DBc.COLLECTION_USER, DBc.COLLECTION_VIDEO]

    def process(self, raw_data, raw_data_filenames=None):
        print("Insert data to DB")
        db_data = raw_data['data']
        # cast from set to list
        courses = db_data.get(DBc.COLLECTION_COURSE)
        users = db_data.get(DBc.COLLECTION_USER)
        if courses:
            for course_info in courses.values():
                course_info[DBc.FIELD_COURSE_STUDENT_IDS] = list(
                    course_info[DBc.FIELD_COURSE_STUDENT_IDS])
        if users:
            for user in users.values():
                user[DBc.FIELD_USER_COURSE_IDS] = list(user[DBc.FIELD_USER_COURSE_IDS])
                user[DBc.FIELD_USER_DROPPED_COURSE_IDS] = list(
                    user[DBc.FIELD_USER_DROPPED_COURSE_IDS])
        # from dictory to list, removing id index
        for (key, value) in db_data.items():
            if isinstance(value, dict):
                db_data[key] = list(value.values())

        # insert to db
        for collection_name in db_data:
            collection = self.db.get_collection(collection_name)
            if collection_name in self.need_drop_collections:
                collection.delete_many({})
            if db_data[collection_name] and len(db_data[collection_name]) > 0:
                collection.insert_many(db_data[collection_name])

        return raw_data


class SetEncoder(json.JSONEncoder):
    # pylint: disable=E0202

    def default(self, obj):

        if type(obj) is set:
            return list(obj)
        elif isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


class OutputFile(PipeModule):


    order = 999

    def __init__(self):
        super().__init__()

    def process(self, raw_data, raw_data_filenames=None):
        db_data = raw_data['data']
        # cast from set to list
        courses = db_data[DBc.COLLECTION_COURSE]
        users = db_data[DBc.COLLECTION_USER]
        for course_info in courses.values():
            course_info[DBc.FIELD_COURSE_STUDENT_IDS] = list(
                course_info[DBc.FIELD_COURSE_STUDENT_IDS])
        for user in users.values():
            user[DBc.FIELD_USER_COURSE_IDS] = list(user[DBc.FIELD_USER_COURSE_IDS])
            user[DBc.FIELD_USER_DROPPED_COURSE_IDS] = list(
                user[DBc.FIELD_USER_DROPPED_COURSE_IDS])
        # from dictory to list, removing id index
        for (key, value) in db_data.items():
            if isinstance(value, dict):
                db_data[key] = list(value.values())
        write_file = open('/vismooc-test-data/newData/processed_data.json', 'w')
        write_file.write(json.dumps(db_data, cls=SetEncoder))
        write_file.close()
        return raw_data

class ExtractRawData(PipeModule):
    """ Preprocess the file to extract different lines for different tables
    """
    order = 0

    def __init__(self):
        super().__init__()

    def process(self, raw_data, raw_data_filenames=None):
        pattern_insert = r'^INSERT INTO `(?P<table_name>\w*)`'
        pattern_create_db = r'^USE `(?P<db_name>\w*)`;$'
        re_pattern_insert_table = re.compile(pattern_insert)
        re_pattern_create_db = re.compile(pattern_create_db)
        current_db = None
        structureIds = set()
        structureId_to_courseId = {}
        courseId_to_structure = {}
        block_queue  = queue.Queue()

        module_structure_filename = None

        print("Processing ExtractRawData")

        for filename in raw_data_filenames:
            if FilenameConfig.SQLDB_Name in filename:
                with open(filename, 'r', encoding='utf-8') as file:
                    for line in file:
                        match_db = re_pattern_create_db.search(line)
                        # find out the current database
                        if match_db is not None:
                            current_db = match_db.group("db_name")
                        if (current_db is None) or (current_db != "edxapp"):
                            continue
                        # if in database edxapp
                        match_table = re_pattern_insert_table.search(line)
                        if match_table is None:
                            continue
                        # remove first '(' and last ';)\n' pay attention to '\n'
                        line = line[line.index('(')+1: -3]
                        records = line.split('),(')
                        table_name = match_table.group("table_name")
                        raw_data[table_name] = records
            elif 'modulestore.active_versions' in filename:
                with open(filename, 'r') as file:
                    for line in file:
                        record = json.loads(line)
                        if record.get('versions').get('published-branch') is None:
                            continue
                        oid = record.get('versions').get('published-branch').get('$oid')
                        structureIds.add(oid)
                        structureId_to_courseId[oid] = record['org'] + '+' + \
                            record['course'] + '+' + record['run']
            elif 'modulestore.structures' in filename:
                module_structure_filename = filename
            elif FilenameConfig.MetaDBRecord_Name in filename:
                with open(filename, 'r') as file:
                    raw_data['data'][DBc.COLLECTION_METADBFILES] = json.load(file)
        # modulestore.active_version must be processed before modulestore.structures
        if module_structure_filename and len(structureId_to_courseId) != 0:
            with open(filename, 'r') as file:
                for line in file:
                    record = json.loads(line)
                    oid = record.get('_id').get('$oid')
                    if oid in structureIds:
                        courseId_to_structure[structureId_to_courseId[oid]] = record
            for one_structure in courseId_to_structure.values():
                blocks_dict = {}
                blocks_to_remove = set()
                # construct a dictory which contains all blocks
                # and get the root course block
                blocks = one_structure.get("blocks")
                for block in blocks:
                    blocks_dict[block.get("block_id")] = block
                    if block.get("block_type") == "course":
                        block_queue.put(block)
                # fill in the children field
                while not block_queue.empty():
                    item = block_queue.get()
                    if item.get("fields") and item.get("fields").get("children"):
                        new_children = []
                        for child in item.get("fields").get("children"):
                            new_children.append(blocks_dict[child[1]])
                            block_queue.put(blocks_dict[child[1]])
                            blocks.remove(blocks_dict.get(child[1]))
                            #blocks_to_remove.add(child[1])
                        item["fields"]["children"] = new_children
            raw_data['course_in_mongo'] = courseId_to_structure
            # with open('/tmp/course_structures.json', 'w') as file:
            #     file.write(json.dumps(courseId_to_structure))
        return raw_data

class PreprocessFormatLogFile():

    order = 3

    def __init__(self):
        super().__init__()

    def load_data(self, data_filenames):
        for filename in data_filenames:
            if '-events-' in filename:
                with open(filename, 'r', encoding='utf-8') as file:
                    raw_data = file.readlines()
                    file.close()
                    yield raw_data

    def process(self, raw_data_filenames=None):
        wrong_username_pattern = r'"username"\s*:\s*"",'
        right_eventsource_pattern = r'"event_source"\s*:\s*"browser"'
        right_match_eventtype_pattern = r'"event_type"\s*:\s*"(hide_transcript|load_video|pause_video|play_video|seek_video|show_transcript|speed_change_video|stop_video|video_hide_cc_menu|video_show_cc_menu)"'

        re_filter_wrong_pattern = re.compile(wrong_username_pattern)
        re_search_right_eventsource_pattern = re.compile(
            right_eventsource_pattern)
        re_search_right_eventtype_pattern = re.compile(
            right_match_eventtype_pattern)
        results = []


        data_to_be_processed = self.load_data(raw_data_filenames)

        for single_file in data_to_be_processed:       
            for line in single_file:
                event_type = re_search_right_eventtype_pattern.search(line)
                if re_filter_wrong_pattern.search(line) is None and re_search_right_eventsource_pattern.search(line) is not None and event_type is not None:
                    results.append(line)
        return results



