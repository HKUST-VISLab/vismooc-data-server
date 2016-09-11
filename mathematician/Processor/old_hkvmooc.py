import re
import json

from ..pipe import PipeModule


class FormatLogFile(PipeModule):

    def __init__(self):
        super().__init__()

    def process(self, raw_data):
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


class FormatUserFile(PipeModule):

    def __init__(self):
        super().__init__()
        self._target_field = {'id', 'username', 'country', 'date_of_birth'}

    def process(self, raw_data):
        processed_data = super().process(raw_data)
        target_field = self._target_field

        headers = processed_data['data'][0][:-1].split('\t')
        headers = [{'index': idx, 'name': header}
                   for idx, header in enumerate(headers) if header in target_field]

        new_processed_data = []
        for row in processed_data['data'][1:]:
            row = row[:-1].split('\t')
            user = {}
            for header in headers:
                user[header['name']] = row[header['index']]
            new_processed_data.append(user)

        processed_data['data'] = new_processed_data
        return processed_data


class FormatVideoFile(PipeModule):

    def __init__(self):
        super().__init__()

    def process(self, raw_data):
        processed_data = super().process(raw_data)

        new_processed_data = []
        course_structure = json.loads(''.join(processed_data['data']))
        for key in course_structure:
            value = course_structure[key]
            if value['category'] == 'video':
                video = {}
                # course id is None
                value_metadata = value['metadata']
                video['name'] = value_metadata['display_name']
                video['url'] = value_metadata['html5_sources']
                video['section'] = value_metadata['sub']
                
                video['metaInfo'] = {}
                youtube_id = value_metadata['youtube_id_1_0']
                if youtube_id is not None:
                    video['metaInfo']['youtube_id'] = youtube_id

                new_processed_data.append(video)

        print(len(new_processed_data))
        processed_data['data'] = new_processed_data
        return processed_data
