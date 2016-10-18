import re
import json
from datetime import timedelta, datetime
from operator import itemgetter
import mathematician.httphelper as httphelper
from bson import ObjectId
from ..pipe import PipeModule
from ..DB.mongo_dbhelper import MongoDB
from ..config import DBConfig as DBc, ThirdPartyKeys

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


class FormatCourseStructFile(PipeModule):

    order = 0

    def __init__(self):
        super().__init__()
        youtube_api_host = 'https://www.googleapis.com/youtube/v3/videos'
        params = 'part=contentDetails&key=' + ThirdPartyKeys.Youtube_key

        self.youtube_api = youtube_api_host + '?' + params

    def load_data(self, data_filenames):
        '''
        Load target file
        '''
        target_filename = None
        for filename in data_filenames:
            if '-course_structure-' in filename:
                target_filename = filename
                break

        if target_filename is not None:
            with open(target_filename, 'r', encoding='utf-8') as file:
                raw_data = ''.join(file.readlines())
                return raw_data

        return None

    def process(self, raw_data, raw_data_filenames=None):

        data_to_be_processed = self.load_data(raw_data_filenames)
        if data_to_be_processed is None:
            return raw_data

        course_structure_info = json.loads(data_to_be_processed)
        videos = {}
        course = {}
        pattern_time = "%Y-%m-%dT%H:%M:%SZ"
        for key in course_structure_info:
            value = course_structure_info[key]
            metadata = value.get('metadata') or {}
            if value['category'] == 'video':
                video = {}
                video_id = key[6:].split('/')
                video_id = key[:3] + '-' + '-'.join(video_id)

                video[DBc.FIELD_VIDEO_COURSE_ID] = None
                video[DBc.FIELD_VIDEO_NAME] = metadata.get('display_name')
                video[DBc.FIELD_VIDEO_TEMPORAL_HOTNESS] = {}
                video[DBc.FIELD_VIDEO_METAINFO] = {}
                video[DBc.FIELD_VIDEO_SECTION] = metadata.get('sub')
                video[DBc.FIELD_VIDEO_RELEASE_DATE] = None
                video[DBc.FIELD_VIDEO_DESCRIPTION] = None
                video[DBc.FIELD_VIDEO_URL] = len(metadata.get(
                    'html5_sources')) and metadata.get('html5_sources')[0]
                video[DBc.FIELD_VIDEO_DURATION] = None
                video[DBc.FIELD_VIDEO_ORIGINAL_ID] = video_id
                video[DBc.FIELD_VIDEO_METAINFO]['youtube_id'] = metadata.get(
                    'youtube_id_1_0')
                videos[video[DBc.FIELD_VIDEO_ORIGINAL_ID]] = video
            elif value['category'] == 'course':
                course_id = key[6:].split('/')
                course_id = course_id[0] + '/' + course_id[1] + '/' + course_id[3]
                start_time = metadata.get('start')
                end_time = metadata.get('end')
                start_time = datetime.strptime(start_time, pattern_time) if start_time else None
                end_time = datetime.strptime(end_time, pattern_time) if end_time else None
                course[DBc.FIELD_COURSE_ORIGINAL_ID] = course_id
                course[DBc.FIELD_COURSE_NAME] = metadata.get('display_name')
                course[DBc.FIELD_COURSE_YEAR] = start_time and start_time.year
                course[DBc.FIELD_COURSE_INSTRUCTOR] = None
                course[DBc.FIELD_COURSE_STATUS] = None
                course[DBc.FIELD_COURSE_URL] = None
                course[DBc.FIELD_COURSE_IMAGE] = metadata.get('course_image')
                course[DBc.FIELD_COURSE_DESCRIPTION] = None
                course[DBc.FIELD_COURSE_METAINFO] = {}
                course[DBc.FIELD_COURSE_STARTTIME] = start_time and start_time.timestamp()
                course[DBc.FIELD_COURSE_ENDTIME] = end_time and end_time.timestamp()
                course[DBc.FIELD_COURSE_STUDENT_IDS] = set()
                course[DBc.FIELD_COURSE_VIDEO_IDS] = set()

        video_youtube_ids = []
        temp_video_dict = {}
        for video in videos.values():
            # video collection is completed
            video[DBc.FIELD_VIDEO_COURSE_ID] = course['originalId']
            # course collection needs studentIds
            course[DBc.FIELD_COURSE_VIDEO_IDS].add(video['originalId'])
            youtube_id = video[DBc.FIELD_VIDEO_METAINFO]['youtube_id']
            video_youtube_ids.append(youtube_id)
            temp_video_dict[youtube_id] = video

        # fetch the video duration from youtube_api_v3
        urls = [self.youtube_api + '&id=' +
                youtube_id for youtube_id in video_youtube_ids]
        broken_youtube_id = set([youtube_id for youtube_id in video_youtube_ids])
        results = httphelper.get_list(urls, limit=60)
        for result in results:
            result = json.loads(result.decode('utf-8'))
            if len(result["items"]) == 0:
                continue
            video_id = result["items"][0]["id"]
            broken_youtube_id.discard(video_id)
            video = temp_video_dict[video_id]
            duration = parse_duration(result["items"][0]["contentDetails"]["duration"])
            video[DBc.FIELD_VIDEO_DURATION] = int(duration.total_seconds())

        processed_data = raw_data
        processed_data['data'][DBc.COLLECTION_VIDEO] = list(videos.values())
        processed_data['data'][DBc.COLLECTION_COURSE] = [course]

        with open("/vismooc-test-data/broken_youtube_id.log", "w+") as f:
            f.write(str(broken_youtube_id))
        return processed_data


class FormatUserFile(PipeModule):

    order = 1

    def __init__(self):
        super().__init__()
        self._userprofile = {}

    def load_data(self, data_filenames):
        '''
        Load target file
        '''
        auth_user_filename = None
        auth_userprofile_filename = None
        for filename in data_filenames:
            if '-auth_user-' in filename:
                auth_user_filename = filename
            elif '-auth_userprofile-' in filename:
                auth_userprofile_filename = filename

        if auth_userprofile_filename is not None:
            with open(auth_userprofile_filename, 'r', encoding='utf-8') as file:
                next(file)
                lines = file.readlines()
                good_lines = []
                for i in reversed(range(len(lines))):
                    if lines[i].startswith('\\n'):
                        lines[i - 1] = lines[i - 1][:-1] + lines[i]
                    else:
                        good_lines.append(lines[i])

                for line in good_lines:
                    line = line[:-1].split('\t')
                    self._userprofile[line[1]] = line

        if auth_user_filename is not None:
            with open(auth_user_filename, 'r', encoding='utf-8') as file:
                next(file)
                raw_data = file.readlines()
                return raw_data
        return None

    def process(self, raw_data, raw_data_filenames=None):
        data_to_be_processed = self.load_data(raw_data_filenames)

        if data_to_be_processed is None:
            return raw_data

        users = {}
        for row in data_to_be_processed:
            row = row[:-1].split('\t')
            user = {}
            user_id = row[0]
            user_profile = self._userprofile.get(user_id)
            user[DBc.FIELD_USER_GENDER] = user_profile and user_profile[7]
            user[DBc.FIELD_USER_COURSE_IDS] = set()
            user[DBc.FIELD_USER_DROPPED_COURSE_IDS] = set()
            user[DBc.FIELD_USER_AGE] = row[16] or (user_profile and user_profile[9])
            user[DBc.FIELD_USER_COUNTRY] = row[14] or (
                user_profile and (user_profile[13] or user_profile[4]))
            user[DBc.FIELD_USER_NAME] = row[1]
            user[DBc.FIELD_USER_ORIGINAL_ID] = user_id
            users[user[DBc.FIELD_USER_ORIGINAL_ID]] = user

        processed_data = raw_data
        # user collection needs courseIds and droppedCourseIds
        processed_data['data'][DBc.COLLECTION_USER] = users

        return processed_data


class FormatEnrollmentFile(PipeModule):

    order = 2
    ENROLL = "enroll"
    UNENROLL = "unenroll"
    action = {'1': ENROLL, '0': UNENROLL}

    def __init__(self):
        super().__init__()

    def load_data(self, data_filenames):
        '''
        Load target file
        '''
        target_filename = None
        for filename in data_filenames:
            if '-student_courseenrollment-' in filename:
                target_filename = filename
                break

        if target_filename is not None:
            with open(target_filename, 'r', encoding='utf-8') as file:
                next(file)
                raw_data = file.readlines()
                return raw_data
        return None

    def process(self, raw_data, raw_data_filenames=None):
        data_to_be_processed = self.load_data(raw_data_filenames)
        if data_to_be_processed is None:
            return raw_data
        course = raw_data['data'][DBc.COLLECTION_COURSE][0]
        users = raw_data['data'][DBc.COLLECTION_USER]

        enrollments = []
        pattern_time = "%Y-%m-%d %H:%M:%S"
        for row in sorted(data_to_be_processed, key=itemgetter(3)):
            row = row[:-1].split('\t')
            enrollment = {}
            user_id = row[1]
            enrollment[DBc.FIELD_ENROLLMENT_COURSE_ID] = row[2]
            enrollment[DBc.FIELD_ENROLLMENT_USER_ID] = user_id
            enrollment[DBc.FIELD_ENROLLMENT_TIMESTAMP] = datetime.strptime(row[3], pattern_time)
            enrollment[DBc.FIELD_ENROLLMENT_ACTION] = FormatEnrollmentFile.action.get(row[4])
            enrollments.append(enrollment)

            # fill user collection
            if users.get(user_id) is not None:
                users[user_id][DBc.FIELD_USER_COURSE_IDS].add(row[2])
            # fill course collection
            if enrollment[DBc.FIELD_ENROLLMENT_ACTION] == FormatEnrollmentFile.ENROLL:
                course[DBc.FIELD_COURSE_STUDENT_IDS].add(row[1])
            else:
                course[DBc.FIELD_COURSE_STUDENT_IDS].discard(row[1])
                users[row[1]][DBc.FIELD_USER_DROPPED_COURSE_IDS].add(row[2])

        processed_data = raw_data
        # course and users collection are completed
        processed_data['data'][DBc.COLLECTION_ENROLLMENT] = enrollments
        processed_data['data'][DBc.COLLECTION_USER] = list(users.values())
        return processed_data


class FormatLogFile(PipeModule):

    order = 3

    def __init__(self):
        super().__init__()

    def try_parsing_date(self, text):
        pattern_time = {"%Y-%m-%dT%H:%M:%S.%f+00:00","%Y-%m-%dT%H:%M:%S+00:00"}
        for pattern in pattern_time:
            try:
                return datetime.strptime(text, pattern)
            except ValueError:
                pass
        raise ValueError('no valid date format found')

    def load_data(self, data_filenames):
        '''
        Load target file
        '''
        target_filename = None
        for filename in data_filenames:
            if '-events-' in filename:
                target_filename = filename
                break

        if target_filename is not None:
            with open(target_filename, 'r', encoding='utf-8') as file:
                raw_data = file.readlines()
                return raw_data
        return None

    def process(self, raw_data, raw_data_filenames=None):
        data_to_be_processed = self.load_data(raw_data_filenames)

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

        temp_video_dict = {video[DBc.FIELD_VIDEO_ORIGINAL_ID]: video for video in raw_data[
            'data'][DBc.COLLECTION_VIDEO]}

        events = []
        broken_event = []
        print('ready to process log file')
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

                temp_data = json.loads("{" + (",".join(temp_data)).replace('.,', ',') + "}")
                event = {}
                event_context = temp_data.get('context') or {}
                event_event = temp_data.get('event') or {}

                video_id = event_event.get('id')
                event_time = self.try_parsing_date(temp_data.get('time'))
                event[DBc.FIELD_VIDEO_LOG_USER_ID] = event_context.get('user_id')
                event[DBc.FIELD_VIDEO_LOG_VIDEO_ID] = video_id
                event[DBc.FIELD_VIDEO_COURSE_ID] = event_context.get('course_id')
                event[DBc.FIELD_VIDEO_LOG_TIMESTAMP] = event_time.timestamp()
                event[DBc.FIELD_VIDEO_LOG_TYPE] = temp_data.get('event_type')

                target_attrs = {'path': 'path', 'code': 'code', 'currentTime': 'currentTime',
                                'new_time': 'newTime', 'old_time': 'oldTime',
                                'new_speed': 'newSpeed', 'old_speed': 'oldSpeed'}

                event[DBc.FIELD_VIDEO_LOG_METAINFO] = {nk: event_event.get(
                    k) for k, nk in target_attrs.items() if event_event.get(k) is not None}
                event[DBc.FIELD_VIDEO_LOG_METAINFO]['path'] = event_context.get('path')

                date_time = str(event_time.date())

                try:
                    temporal_hotness = temp_video_dict[video_id][DBc.FIELD_VIDEO_TEMPORAL_HOTNESS]
                    if date_time not in temporal_hotness:
                        temporal_hotness[date_time] = 0
                    temporal_hotness[date_time] += 1
                except KeyError:
                    broken_event.append(json.dumps(event))
                events.append(event)
        with open("/vismooc-test-data/broken_event.log", "w+") as f:
            f.write(str(broken_event))

        processed_data = raw_data
        processed_data['data'][DBc.COLLECTION_VIDEO_LOG] = events
        print('finish format log file')
        return processed_data


class DumpToDB(PipeModule):
    order = 998

    def __init__(self):
        super().__init__()
        self.db = MongoDB('localhost', 'test-vismooc-java')

    def process(self, raw_data, raw_data_filenames=None):
        db_data = raw_data['data']
        # cast from set to list
        course = db_data[DBc.COLLECTION_COURSE][0]
        course[DBc.FIELD_COURSE_VIDEO_IDS] = list(course[DBc.FIELD_COURSE_VIDEO_IDS])
        course[DBc.FIELD_COURSE_STUDENT_IDS] = list(course[DBc.FIELD_COURSE_STUDENT_IDS])
        users = db_data[DBc.COLLECTION_USER]
        for user in users:
            user[DBc.FIELD_USER_COURSE_IDS] = list(user[DBc.FIELD_USER_COURSE_IDS])
            user[DBc.FIELD_USER_DROPPED_COURSE_IDS] = list(
                user[DBc.FIELD_USER_DROPPED_COURSE_IDS])

        for video in db_data[DBc.COLLECTION_VIDEO]:
            temp_hotness = video[DBc.FIELD_VIDEO_TEMPORAL_HOTNESS]
            video[DBc.FIELD_VIDEO_TEMPORAL_HOTNESS] = [
                {"date": k, "value": v} for k, v in temp_hotness.items()]
        
        # insert to db
        print('ready to dump to db')
        for collection_name in db_data:
            collection = self.db.get_collection(collection_name)
            if db_data[collection_name] is not None:
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
        write_file = open('./test-data/processed_data.json', 'w')
        write_file.write(json.dumps(raw_data, cls=SetEncoder))
        write_file.close()
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
