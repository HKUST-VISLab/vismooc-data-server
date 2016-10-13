import re
import json
import mathematician.httphelper as httphelper
from datetime import timedelta
from operator import itemgetter
from bson import ObjectId
from ..pipe import PipeModule
from ..DB.mongo_dbhelper import MongoDB
from ..config import DBConfig, ThirdPartyKeys

ISO_8601_duration_rx = re.compile(
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
    match = ISO_8601_duration_rx.match(datestring)
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
        target_filename = None
        for filename in data_filenames:
            if '-course_structure-' in filename:
                target_filename = filename
                break

        if target_filename is not None:
            with open(target_filename, 'r', encoding='utf-8') as file:
                raw_data = ''.join(file.readlines())
                file.close()
                return raw_data

        return None

    def process(self, raw_data, raw_data_filenames=None):

        data_to_be_processed = self.load_data(raw_data_filenames)
        if data_to_be_processed is None:
            return raw_data

        course_structure_info = json.loads(data_to_be_processed)
        videos = {}
        course = {}
        for key in course_structure_info:
            value = course_structure_info[key]
            value_metadata = value.get('metadata') or {}
            if value['category'] == 'video':
                video = {}
                video_id = key[6:].split('/')
                video_id = key[:3] + '-' + '-'.join(video_id)

                video[DBConfig.FIELD_VIDEO_COURSE_ID] = None
                video[DBConfig.FIELD_VIDEO_NAME] = value_metadata.get('display_name')
                video[DBConfig.FIELD_VIDEO_TEMPORAL_HOTNESS] = None
                video[DBConfig.FIELD_VIDEO_METAINFO] = {}
                video[DBConfig.FIELD_VIDEO_SECTION] = value_metadata.get('sub')
                video[DBConfig.FIELD_VIDEO_RELEASE_DATE] = None
                video[DBConfig.FIELD_VIDEO_DESCRIPTION] = None
                video[DBConfig.FIELD_VIDEO_URL] = len(value_metadata.get(
                    'html5_sources')) and value_metadata.get('html5_sources')[0]
                video[DBConfig.FIELD_VIDEO_DURATION] = None
                video[DBConfig.FIELD_VIDEO_ORIGINAL_ID] = video_id
                video[DBConfig.FIELD_VIDEO_METAINFO]['youtube_id'] = value_metadata.get(
                    'youtube_id_1_0')
                videos[video[DBConfig.FIELD_VIDEO_ORIGINAL_ID]] = video
            elif value['category'] == 'course':
                course_id = key[6:].split('/')
                course_id = course_id[0] + '/' + \
                    course_id[1] + '/' + course_id[3]
                course[DBConfig.FIELD_COURSE_NAME] = value_metadata.get('display_name')
                course[DBConfig.FIELD_COURSE_YEAR] = None
                course[DBConfig.FIELD_COURSE_INSTRUCTOR] = None
                course[DBConfig.FIELD_COURSE_STATUS] = None
                course[DBConfig.FIELD_COURSE_URL] = None
                course[DBConfig.FIELD_COURSE_IMAGE] = value_metadata.get('course_image')
                course[DBConfig.FIELD_COURSE_DESCRIPTION] = None
                course[DBConfig.FIELD_COURSE_METAINFO] = {}
                course[DBConfig.FIELD_COURSE_STARTTIME] = value_metadata.get('start')
                course[DBConfig.FIELD_COURSE_ENDTIME] = value_metadata.get('end')
                course[DBConfig.FIELD_COURSE_ORIGINAL_ID] = course_id
                course[DBConfig.FIELD_COURSE_STUDENT_LIST] = set()
                course[DBConfig.FIELD_COURSE_VIDEO_LIST] = set()

        video_youtube_ids = []
        temp_youtube_id_video_dict = {}
        for video in videos.values():
            # video collection is completed
            video[DBConfig.FIELD_VIDEO_COURSE_ID] = course['originalId']
            # course collection needs studentIds
            course[DBConfig.FIELD_COURSE_VIDEO_LIST].add(video['originalId'])
            youtube_id = video[DBConfig.FIELD_VIDEO_METAINFO]['youtube_id']
            video_youtube_ids.append(youtube_id)
            temp_youtube_id_video_dict[youtube_id] = video
        
        # fetch the video duration from youtube_api_v3
        urls = [self.youtube_api + '&id=' +
                youtube_id for youtube_id in video_youtube_ids]
        results = httphelper.get_list(urls, limit=60)
        for result in results:
            video_id = result["items"][0]["id"]
            video = temp_youtube_id_video_dict[video_id]
            duration = parse_duration(result["items"][0]["contentDetails"]["duration"])
            video[DBConfig.FIELD_VIDEO_DURATION] = int(duration.total_seconds())

        processed_data = raw_data
        processed_data['data'][DBConfig.COLLECTION_VIDEO] = list(videos.values())
        processed_data['data'][DBConfig.COLLECTION_COURSE] = [course]

        return processed_data


class FormatUserFile(PipeModule):

    order = 1

    def __init__(self):
        super().__init__()
        self._userprofile = {}

    def load_data(self, data_filenames):
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
                file.close()
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
            user[DBConfig.FIELD_USER_GENDER] = user_profile and user_profile[7]
            user[DBConfig.FIELD_USER_COURSE_LIST] = set()
            user[DBConfig.FIELD_USER_DROPPED_COURSE_LIST] = set()
            user[DBConfig.FIELD_USER_AGE] = row[16] or (user_profile and user_profile[9])
            user[DBConfig.FIELD_USER_COUNTRY] = row[14] or (
                user_profile and (user_profile[13] or user_profile[4]))
            user[DBConfig.FIELD_USER_NAME] = row[1]
            user[DBConfig.FIELD_USER_ORIGINAL_ID] = user_id
            users[user[DBConfig.FIELD_USER_ORIGINAL_ID]] = user

        processed_data = raw_data
        # user collection needs courseIds and droppedCourseIds
        processed_data['data'][DBConfig.COLLECTION_USER] = users

        return processed_data


class FormatEnrollmentFile(PipeModule):

    order = 2
    action = {1: 'enroll', 0: 'unenroll'}

    def __init__(self):
        super().__init__()

    def load_data(self, data_filenames):
        target_filename = None
        for filename in data_filenames:
            if '-student_courseenrollment-' in filename:
                target_filename = filename
                break

        if target_filename is not None:
            with open(target_filename, 'r', encoding='utf-8') as file:
                next(file)
                raw_data = file.readlines()
                file.close()
                return raw_data
        return None

    def process(self, raw_data, raw_data_filenames=None):
        data_to_be_processed = self.load_data(raw_data_filenames)
        if data_to_be_processed is None:
            return raw_data
        course = raw_data['data'][DBConfig.COLLECTION_COURSE][0]
        users = raw_data['data'][DBConfig.COLLECTION_USER]

        enrollments = []
        for row in sorted(data_to_be_processed, key=itemgetter(3)):
            row = row[:-1].split('\t')
            enrollment = {}
            user_id = row[1]
            enrollment[DBConfig.FIELD_ENROLLMENT_COURSE_ID] = row[2]
            enrollment[DBConfig.FIELD_ENROLLMENT_USER_ID] = user_id
            enrollment[DBConfig.FIELD_ENROLLMENT_TIMESTAMP] = row[3]
            enrollment[DBConfig.FIELD_ENROLLMENT_ACTION] = FormatEnrollmentFile.action.get(row[4])
            enrollments.append(enrollment)

            # fill user collection
            if users.get(user_id) is not None:
                users[user_id][DBConfig.FIELD_USER_COURSE_LIST].add(row[2])
            # fill course collection
            if enrollment[DBConfig.FIELD_ENROLLMENT_ACTION] == 1:
                course[DBConfig.FIELD_COURSE_STUDENT_LIST].add(row[1])
            else:
                course[DBConfig.FIELD_COURSE_STUDENT_LIST].discard(row[1])
                users[row[1]][DBConfig.FIELD_USER_DROPPED_COURSE_LIST].add(row[2])

        processed_data = raw_data
        # course and users collection are completed
        processed_data['data'][DBConfig.COLLECTION_ENROLLMENT] = enrollments
        processed_data['data'][DBConfig.COLLECTION_USER] = list(users.values())
        return processed_data


class FormatLogFile(PipeModule):

    order = 3

    def __init__(self):
        super().__init__()

    def load_data(self, data_filenames):
        target_filename = None
        for filename in data_filenames:
            if '-events-' in filename:
                target_filename = filename
                break

        if target_filename is not None:
            with open(target_filename, 'r', encoding='utf-8') as file:
                raw_data = file.readlines()
                file.close()
                return raw_data
        return None

    def process(self, raw_data, raw_data_filenames=None):
        data_to_be_processed = self.load_data(raw_data_filenames)

        if data_to_be_processed is None:
            return raw_data

        wrong_username_pattern = r'"username"\s*:\s*"",'
        right_eventsource_pattern = r'"event_source"\s*:\s*"browser"'
        right_match_eventtype_pattern = r'"event_type"\s*:\s*"(hide_transcript|load_video|pause_video|play_video|seek_video|show_transcript|speed_change_video|stop_video|video_hide_cc_menu|video_show_cc_menu)"'

        match_context_pattern = r',?\s*("context"\s*:\s*{[^}]*})'
        match_event_pattern = r',?\s*("event"\s*:\s*"([^"]|\\")*(?<!\\)")'
        match_username_pattern = r',?\s*("username"\s*:\s*"[^"]*")'
        match_time_pattern = r',?\s*("time"\s*:\s*"[^"]*")'
        match_event_json_escape_pattern = r'"(?={)|"$'
        re_filter_wrong_pattern = re.compile(wrong_username_pattern)
        re_search_right_eventsource_pattern = re.compile(
            right_eventsource_pattern)
        re_search_right_eventtype_pattern = re.compile(
            right_match_eventtype_pattern)
        re_search_context_pattern = re.compile(match_context_pattern)
        re_search_event_pattern = re.compile(match_event_pattern)
        re_search_username_pattern = re.compile(match_username_pattern)
        re_search_time_pattern = re.compile(match_time_pattern)
        re_search_event_json_escape_pattern = re.compile(
            match_event_json_escape_pattern)

        events = []
        for line in data_to_be_processed:
            event_type = re_search_right_eventtype_pattern.search(line)
            if re_filter_wrong_pattern.search(line) is None and re_search_right_eventsource_pattern.search(line) is not None and event_type is not None:
                context = re_search_context_pattern.search(line)
                event_field = re_search_event_pattern.search(line)
                username = re_search_username_pattern.search(line)
                timestamp = re_search_time_pattern.search(line)
                temp_array = [event_type.group()]
                if context is not None:
                    temp_array.append(context.group(1))
                if event_field is not None:
                    temp_array.append(re_search_event_json_escape_pattern.sub(
                        '', event_field.group(1).replace('\\', '')))
                if username is not None:
                    temp_array.append(username.group(1))
                if timestamp is not None:
                    temp_array.append(timestamp.group(1))

                json_str = "{" + ",".join(temp_array) + "}"
                event_json = json.loads(json_str)
                event = {}
                event_context = event_json.get('context')
                event[DBConfig.FIELD_VIDEO_LOG_USER_ID] = event_context and event_context['user_id']
                event[DBConfig.FIELD_VIDEO_LOG_VIDEO_ID] = event_json.get(
                    'event') and event_json.get('event')['id']
                event[DBConfig.FIELD_VIDEO_LOG_TIMESTAMP] = event_json.get('time')
                event[DBConfig.FIELD_VIDEO_LOG_TYPE] = event_json.get('event_type')
                event[DBConfig.FIELD_VIDEO_LOG_METAINFO] = event_json.get('event')

                events.append(event)

        processed_data = raw_data
        processed_data['data'][DBConfig.COLLECTION_VIDEO_LOG] = events
        return processed_data


class DumpToDB(PipeModule):
    order = 998

    def __init__(self):
        super().__init__()
        self.db = MongoDB('localhost', 'test-vismooc-java')

    def process(self, raw_data, raw_data_filenames=None):
        db_data = raw_data['data']
        # cast from set to list
        course = db_data[DBConfig.COLLECTION_COURSE][0]
        course[DBConfig.FIELD_COURSE_VIDEO_LIST] = list(course[DBConfig.FIELD_COURSE_VIDEO_LIST])
        course[DBConfig.FIELD_COURSE_STUDENT_LIST] = list(
            course[DBConfig.FIELD_COURSE_STUDENT_LIST])
        users = db_data[DBConfig.COLLECTION_USER]
        for user in users:
            user[DBConfig.FIELD_USER_COURSE_LIST] = list(user[DBConfig.FIELD_USER_COURSE_LIST])
            user[DBConfig.FIELD_USER_DROPPED_COURSE_LIST] = list(
                user[DBConfig.FIELD_USER_DROPPED_COURSE_LIST])

        # insert to db
        for collection_name in db_data:
            collection = self.db.get_collection(collection_name)
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
