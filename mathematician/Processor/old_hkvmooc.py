import re
import json
from operator import itemgetter
from bson import ObjectId
from ..pipe import PipeModule
from ..DB.mongo_dbhelper import MongoDB


class FormatCourseStructFile(PipeModule):

    order = 0

    def __init__(self):
        super().__init__()

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
                video['courseId'] = None
                video['name'] = value_metadata.get('display_name')
                video['temproalHotness'] = None
                video['metaInfo'] = {}
                video['section'] = value_metadata.get('sub')
                video['releaseDate'] = None
                video['description'] = None
                video['url'] = len(value_metadata.get('html5_sources')) and value_metadata.get(
                    'html5_sources')[0]
                video['length'] = None
                video['originalId'] = video_id
                video['metaInfo']['youtube_id'] = value_metadata.get(
                    'youtube_id_1_0')
                videos[video['originalId']] = video
            elif value['category'] == 'course':
                course_id = key[6:].split('/')
                course_id = course_id[0] + '/' + \
                    course_id[1] + '/' + course_id[3]
                course['name'] = value_metadata.get('display_name')
                course['year'] = None
                course['instructor'] = None
                course['status'] = None
                course['url'] = None
                course['image'] = value_metadata.get('course_image')
                course['description'] = None
                course['metaInfo'] = {}
                course['startDate'] = value_metadata.get('start')
                course['endDate'] = value_metadata.get('end')
                course['originalId'] = course_id
                course['studentIds'] = set()
                course['videoIds'] = set()

        for video in videos.values():
            # video collection is completed
            video['courseId'] = course['originalId']
            # course collection needs studentIds
            course['videoIds'].add(video['originalId'])

        processed_data = raw_data
        processed_data['data']['videos'] = list(videos.values())
        processed_data['data']['courses'] = [course]

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
            user['gender'] = user_profile and user_profile[7]
            user['courseIds'] = set()
            user['droppedCourseIds'] = set()
            user['age'] = row[16] or (user_profile and user_profile[9])
            user['country'] = row[14] or (
                user_profile and (user_profile[13] or user_profile[4]))
            user['username'] = row[1]
            user['originalId'] = user_id
            users[user['originalId']] = user

        processed_data = raw_data
        # user collection needs courseIds and droppedCourseIds
        processed_data['data']['users'] = users

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
        course = raw_data['data']['courses'][0]
        users = raw_data['data']['users']

        enrollments = []
        for row in sorted(data_to_be_processed, key=itemgetter(3)):
            row = row[:-1].split('\t')
            enrollment = {}
            user_id = row[1]
            enrollment['courseId'] = row[2]
            enrollment['userId'] = user_id
            enrollment['timestamp'] = row[3]
            enrollment['action'] = FormatEnrollmentFile.action.get(row[4])
            enrollments.append(enrollment)
            # fill user collection
            if users.get(user_id) is not None:
                users[user_id]['courseIds'].add(row[2])
            # fill course collection
            if enrollment['action'] == 1:
                course['studentIds'].add(row[1])
            else:
                course['studentIds'].discard(row[1])
                users[row[1]]['droppedCourseIds'].add(row[2])

        processed_data = raw_data
        # course and users collection are completed
        processed_data['data']['enrollments'] = enrollments
        processed_data['data']['users'] = list(users.values())
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
                event['userId'] = event_context and event_context['user_id']

                event['videoId'] = event_json.get(
                    'event') and event_json.get('event')['id']

                event['timestamp'] = event_json.get('time')
                event['type'] = event_json.get('event_type')
                event['metaInfo'] = event_json.get('event')
                events.append(event)

        processed_data = raw_data
        processed_data['data']['events'] = events
        return processed_data


class DumpToDB(PipeModule):
    order = 998

    def __init__(self):
        super().__init__()
        self.db = MongoDB('localhost', 'test-vismooc-java')

    def process(self, raw_data, raw_data_filenames=None):
        db_data = raw_data['data']
        # cast from set to list
        course = db_data['courses'][0]
        course['videoIds'] = list(course['videoIds'])
        course['studentIds'] = list(course['studentIds'])
        users = db_data['users']
        for user in users:
            user['courseIds'] = list(user['courseIds'])
            user['droppedCourseIds'] = list(user['droppedCourseIds'])
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
