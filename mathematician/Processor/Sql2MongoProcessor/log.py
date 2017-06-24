'''Process the log data from MoocDB to MongoDB
'''
import re

from mathematician.config import DBConfig as DBc
from mathematician.logger import info, warn
from mathematician.pipe import PipeModule
from mathematician.Processor.Sql2MongoProcessor.constant import RD_DATA
from mathematician.Processor.utils import (get_data_by_table,
                                           round_timestamp_to_day,
                                           try_get_timestamp)


class LogProcessor(PipeModule):
    '''The Processor to process log data from MoocDB to MongoDB
    '''
    order = 6
    field2index = {'currentTime': 7,
                   'newTime': 8, 'oldTime': 9,
                   'newSpeed': 10, 'oldSpeed': 11}

    def __init__(self):
        super().__init__()
        self.sql_table = 'click_events'
        self.events = []
        self.denselogs = {}

    def load_data(self):
        '''Load target file
        '''
        data = get_data_by_table(self.sql_table)
        return data

    def process(self, raw_data, raw_data_filenames=None):
        '''Process the data
        '''
        info("Processing log files")
        all_data_to_be_processed = self.load_data()

        if all_data_to_be_processed is None:
            return raw_data

        videos = raw_data[RD_DATA].get(DBc.COLLECTION_VIDEO)
        for row in all_data_to_be_processed:

            # ugly hacked. TODO
            course_id = row[4] if len(row) > 12 else 'org:HKUSTx/COMP102x/2T2014'
            if course_id is None:
                continue
            try:
                course_id = course_id[course_id.index(':') + 1:]
            except ValueError as ex:
                warn("In ParseLogFile, cannot get courseId of:" + course_id)
                warn(ex)
                continue
            course_id = re.sub(r'[\.|\/|\+]', '_', course_id)

            event = {}
            event[DBc.FIELD_LOG_USER_ID] = row[1]
            event[DBc.FIELD_LOG_VIDEO_ID] = row[2]
            event[DBc.FIELD_LOG_TIMESTAMP] = try_get_timestamp(row[3])
            event[DBc.FIELD_LOG_COURSE_ID] = course_id
            event[DBc.FIELD_LOG_TYPE] = row[6]
            event[DBc.FIELD_LOG_METAINFO] = {k: row[v]
                                             for (k, v) in LogProcessor.field2index.items() if row[v] is not None}
            self.events.append(event)

            video_id = event[DBc.FIELD_LOG_VIDEO_ID]
            event_date = round_timestamp_to_day(event[DBc.FIELD_LOG_TIMESTAMP])
            denselogs_key = "{0}_{1}_{2}".format(course_id, video_id, event_date)

            if self.denselogs.get(denselogs_key) is None:
                self.denselogs[denselogs_key] = {
                    DBc.FIELD_DENSELOGS_COURSE_ID: course_id,
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

        return processed_data
