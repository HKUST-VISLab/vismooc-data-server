'''Process the log raw files
'''
import json
import multiprocessing
import re

from mathematician.config import DBConfig as DBc
from mathematician.logger import info, warn
from mathematician.pipe import PipeModule
from mathematician.Processor.Rawfile2MongoProcessor.constant import RD_DATA
from mathematician.Processor.utils import (PARALLEL_GRAIN, get_cpu_num,
                                           is_processed,
                                           round_timestamp_to_day,
                                           try_get_timestamp, try_parse_date)


class LogProcessor(PipeModule):
    '''The processor to process the log raw files
    '''

    order = 6
    pattern_right_eventsource = r'"event_source"\s*:\s*"browser"'
    pattern_right_eventtype = r'"event_type"\s*:\s*"(pause_video|' + \
        r'play_video|seek_video|speed_change_video|stop_video)"'

    pattern_context = r',?\s*("context"\s*:\s*{[^}]*})'
    pattern_event = r',?\s*("event"\s*:\s*"([^"]|\\")*(?<!\\)")'
    pattern_username = r',?\s*("username"\s*:\s*"[^"]*")'
    pattern_time = r',?\s*("time"\s*:\s*"[^"]*")'
    pattern_event_json_escape_ = r'"(?={)|"$'
    pattern_hash = r'[0-9a-f]{32}'
    pattern_date = "%Y-%m-%dT%H:%M:%S.%f+00:00"

    re_right_eventsource = re.compile(pattern_right_eventsource)
    re_right_eventtype = re.compile(pattern_right_eventtype)
    re_context = re.compile(pattern_context)
    re_event = re.compile(pattern_event)
    re_username = re.compile(pattern_username)
    re_time = re.compile(pattern_time)
    re_event_json_escape = re.compile(pattern_event_json_escape_)
    re_pattern_hash = re.compile(pattern_hash)

    def __init__(self):
        super().__init__()
        self.processed_files = []
        self.events = []
        self.denselogs = {}

    def load_data(self, data_filenames):
        '''Load target file
        '''
        for filename in data_filenames:
            if '-clickstream' in filename and not is_processed(filename):
                with open(filename, 'r', encoding='utf-8') as file:
                    raw_data = file.readlines()
                    self.processed_files.append(filename)
                    yield raw_data

    @staticmethod
    def is_target_log(line):
        '''Detect if the log is target log. If so, return the event_type. Otherise, return None
        '''
        if not isinstance(line, str):
            return None

        if LogProcessor.re_right_eventsource.search(line) is None:
            return None

        event_type = LogProcessor.re_right_eventtype.search(line)
        if event_type is None:
            return None

        username = LogProcessor.re_username.search(line)
        if username is None or len(username.group(1).replace('"username":', '').strip()) == 2:
            return None

        context = LogProcessor.re_context.search(line)
        if context is None:
            return None

        timestamp = LogProcessor.re_time.search(line)
        if timestamp is None:
            return None

        event_field = LogProcessor.re_event.search(line)
        if event_field is None:
            return None
        event_field = LogProcessor.re_event_json_escape.sub(
            '', event_field.group(1).replace('\\', ''))

        return [event_type.group(), username.group(1), context.group(1), event_field, timestamp.group(1)]

    @staticmethod
    def process_few_logs(lines):
        '''Process one piece of log
        '''
        events = []
        for line in lines:
            temp_data = LogProcessor.is_target_log(line)
            if temp_data is None:
                continue

            str_temp_data = "{" + ",".join(temp_data) + "}"
            str_temp_data = str_temp_data.replace('.,', ',', 1)
            try:
                dict_temp_data = json.loads(str_temp_data)
            except json.JSONDecodeError as ex:
                warn("In LogProcessor, cannot json.loads(str_temp_data)")
                warn(ex)
                continue

            event_context = dict_temp_data.get('context') or {}
            event_event = dict_temp_data.get('event') or {}

            # ready for event
            event = {}
            # video_id
            video_id = event_event.get('id')
            if video_id is None:
                continue
            video_id = LogProcessor.re_pattern_hash.search(video_id).group()
            event[DBc.FIELD_LOG_VIDEO_ID] = video_id
            # course_id
            course_id = event_context.get('course_id')
            if course_id is None:
                continue
            try:
                course_id = course_id[course_id.index(':') + 1:]
            except ValueError as ex:
                warn("In ParseLogFile, cannot get courseId of:" + course_id)
                warn(ex)
                continue
            course_id = re.sub(r'[\.|\/|\+]', '_', course_id)
            event[DBc.FIELD_LOG_COURSE_ID] = course_id
            # event_time, has been checked in is_target_log
            str_event_time = dict_temp_data.get('time')
            if '.' not in str_event_time:
                try:
                    str_event_time = str_event_time[:str_event_time.index("+")] + \
                        '.000000' + str_event_time[str_event_time.index("+"):]
                except ValueError as ex:
                    warn("In ParseLogFile, cannot process the event_time:" + str_event_time)
                    warn(ex)
                    continue
            event_time = try_parse_date(str_event_time, LogProcessor.pattern_date)
            event[DBc.FIELD_LOG_TIMESTAMP] = try_get_timestamp(event_time)
            # user_id, could be None
            event[DBc.FIELD_LOG_USER_ID] = event_context.get('user_id')
            # event_type, has been checked in is_target_log
            event[DBc.FIELD_LOG_TYPE] = dict_temp_data.get('event_type')
            # meta_info
            target_attrs = {'currentTime': 'currentTime', 'new_time': 'newTime',
                            'old_time': 'oldTime', 'new_speed': 'newSpeed',
                            'old_speed': 'oldSpeed'}
            event[DBc.FIELD_LOG_METAINFO] = {target_attrs[k]: event_event.get(
                k) for k in target_attrs if event_event.get(k) is not None}
            events.append(event)

        return events

    def process(self, raw_data, raw_data_filenames=None):
        info("Processing log files")
        if raw_data_filenames is None:
            return raw_data

        all_data_to_be_processed = self.load_data(raw_data_filenames)
        if all_data_to_be_processed is None:
            return raw_data

        cpu_num = get_cpu_num()
        videos = raw_data[RD_DATA].get(DBc.COLLECTION_VIDEO)
        for data_to_be_processed in all_data_to_be_processed:
            data = [data_to_be_processed[l: l + PARALLEL_GRAIN]
                    for l in range(0, len(data_to_be_processed), PARALLEL_GRAIN)]
            pool = multiprocessing.Pool(processes=cpu_num)
            results = pool.map_async(LogProcessor.process_few_logs, data)
            pool.close()
            pool.join()
            for few_events in results.get():
                if len(few_events) <= 0:
                    continue
                self.events.extend(few_events)
                for event in few_events:
                    # for denselog
                    course_id = event[DBc.FIELD_LOG_COURSE_ID]
                    video_id = event[DBc.FIELD_LOG_VIDEO_ID]
                    event_date = round_timestamp_to_day(event[DBc.FIELD_LOG_TIMESTAMP])
                    denselogs_key = "{0}_{1}_{2}".format(course_id, video_id, event_date)

                    if self.denselogs.get(denselogs_key) is None:
                        self.denselogs[denselogs_key] = {
                            DBc.FIELD_DENSELOGS_COURSE_ID: event[DBc.FIELD_LOG_COURSE_ID],
                            DBc.FIELD_DENSELOGS_VIDEO_ID: video_id,
                            DBc.FIELD_DENSELOGS_TIMESTAMP: event_date,
                            DBc.FIELD_DENSELOGS_CLICKS: []
                        }
                    click = {
                        DBc.FIELD_DENSELOGS_USER_ID: event[DBc.FIELD_LOG_USER_ID],
                        DBc.FIELD_DENSELOGS_TYPE: event[DBc.FIELD_LOG_TYPE]
                    }
                    click.update(event[DBc.FIELD_LOG_METAINFO])
                    self.denselogs[denselogs_key][DBc.FIELD_DENSELOGS_CLICKS].append(click)
                    # augment videos
                    if videos and isinstance(videos.get(video_id), dict):
                        temporal_hotness = videos[video_id].get(DBc.FIELD_VIDEO_TEMPORAL_HOTNESS)
                        if temporal_hotness is None:
                            temporal_hotness = {}
                            videos[video_id][DBc.FIELD_VIDEO_TEMPORAL_HOTNESS] = temporal_hotness
                        if event_date not in temporal_hotness:
                            temporal_hotness[event_date] = 0
                        temporal_hotness[event_date] += 1

        processed_data = raw_data
        processed_data[RD_DATA][DBc.COLLECTION_LOG] = self.events
        processed_data[RD_DATA][DBc.COLLECTION_DENSELOGS] = list(self.denselogs.values())
        processed_data.setdefault('processed_files', []).extend(self.processed_files)

        return processed_data
