import re
import json

from ..pipe import PipeModule

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
        videos = []
        course = {}
        for key in course_structure_info:
            value = course_structure_info[key]
            value_metadata = value.get('metadata') or {}
            if value['category'] == 'video':
                video = {}
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
                video['originalId'] = key
                video['metaInfo']['youtube_id'] = value_metadata.get(
                    'youtube_id_1_0')

                videos.append(video)
            elif value['category'] == 'course':
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
                course['originalId'] = key
                course['studentIds'] = []
                course['videoIds'] = []

        for video in videos:
            video['courseId'] = course['originalId'] # video collection is complete here
            course['videoIds'].append(video['originalId']) # course collection needs studentIds

        processed_data = raw_data
        processed_data['data']['videos'] = videos
        processed_data['data']['course'] = course

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

        users = []
        for row in data_to_be_processed:
            row = row[:-1].split('\t')
            user = {}
            user_id = row[0]
            user_profile = self._userprofile.get(user_id)
            user['gender'] = user_profile and user_profile[7]
            user['courseList'] = []
            user['dropedCourseList'] = []
            user['age'] = row[16] or (user_profile and user_profile[9])
            user['country'] = row[14] or (
                user_profile and (user_profile[13] or user_profile[4]))
            user['username'] = row[1]
            user['originalId'] = user_id
            users.append(user)

        processed_data = raw_data
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
        enrollments = []
        for row in data_to_be_processed:
            enrollment = {}
            enrollment['courseId'] = row[2]
            enrollment['userId'] = row[1]
            enrollment['timestamp'] = row[3]
            enrollment['action'] = FormatEnrollmentFile.action.get(row[4])
            enrollments.append(enrollment)

        processed_data = raw_data
        processed_data['data']['enrollments'] = enrollments
        return processed_data

class FormatLogFile(PipeModule):

    order = 3
    def __init__(self):
        super().__init__()

    def process(self, raw_data, raw_data_filenames=None):
        processed_data = super().process(raw_data)

        wrong_username_pattern = r'"username"\s*:\s*"",'
        right_eventsource_pattern = r'"event_source"\s*:\s*"browser"'
        right_match_eventtype_pattern = r'"event_type"\s*:\s*"(hide_transcript|load_video|pause_video|play_video|seek_video|show_transcript|speed_change_video|stop_video|video_hide_cc_menu|video_show_cc_menu)"'

        match_context_pattern = r',?\s*("context"\s*:\s*{[^}]*})'
        match_event_pattern = r',?\s*("event"\s*:\s*"([^"]|\\")*(?<!\\)")'
        match_username_pattern = r',?\s*("username"\s*:\s*"[^"]*")'
        match_time_pattern = r',?\s*("time"\s*:\s*"[^"]*")'

        re_filter_wrong_pattern = re.compile(wrong_username_pattern)
        re_search_right_eventsource_pattern = re.compile(
            right_eventsource_pattern)
        re_search_right_eventtype_pattern = re.compile(
            right_match_eventtype_pattern)
        re_search_context_pattern = re.compile(match_context_pattern)
        re_search_event_pattern = re.compile(match_event_pattern)
        re_search_username_pattern = re.compile(match_username_pattern)
        re_search_time_pattern = re.compile(match_time_pattern)

        new_processed_data = []
        for line in processed_data['data']:
            event_type = re_search_right_eventtype_pattern.search(line)
            if re_filter_wrong_pattern.search(line) is None and re_search_right_eventsource_pattern.search(line) is not None and event_type is not None:
                context = re_search_context_pattern.search(line)
                event = re_search_event_pattern.search(line)
                username = re_search_username_pattern.search(line)
                timestamp = re_search_time_pattern.search(line)
                temp_array = [event_type.group()]
                if context:
                    temp_array.append(context.group(1))
                if event:
                    temp_array.append(event.group(1))
                if username:
                    temp_array.append(username.group(1))
                if timestamp:
                    temp_array.append(timestamp.group(1))

                json_str = "{" + ",".join(temp_array) + "}"
                new_processed_data.append(json.loads(json_str))
        processed_data['data'] = new_processed_data

        return processed_data
