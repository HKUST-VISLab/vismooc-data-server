'''Unit test for LogProcessor
'''
# pylint: disable=C0111, C0103
import unittest
from unittest.mock import patch

from mathematician.pipe import PipeModule
from mathematician.Processor.Sql2MongoProcessor import LogProcessor
from mathematician.config import DBConfig as DBc
from mathematician.Processor.utils import try_parse_date, round_timestamp_to_day, try_get_timestamp


def get_right_log(event_type='play_video'):
    # pylint: disable=C0301
    return r'{"username": "samast", "event_type": "' + event_type + r'", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"new_time\":1,\"old_time\":1,\"new_speed\":2,\"old_speed\":2,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "org:HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'

def get_log_expected_return(event_type='play_video'):
    expectd = {}
    expectd[DBc.FIELD_LOG_USER_ID] = 4540165
    expectd[DBc.FIELD_LOG_VIDEO_ID] = '17df731f26364049908a8c227cb4c3c3'
    expectd[DBc.FIELD_LOG_COURSE_ID] = 'HKUSTx_COMP102x_2T2014'
    expectd[DBc.FIELD_LOG_TIMESTAMP] = try_get_timestamp(try_parse_date(
        "2014-08-23T04:57:33.502967+00:00", "%Y-%m-%dT%H:%M:%S.%f+00:00"))
    expectd[DBc.FIELD_LOG_TYPE] = event_type
    expectd[DBc.FIELD_LOG_METAINFO] = {
        'path': '/path',
        "currentTime": 79,
        "newTime": 1,
        "oldTime": 1,
        "newSpeed": 2,
        "oldSpeed": 2
    }
    return expectd

def get_denselog_expected_return(event, count):
    click = {}
    click[DBc.FIELD_DENSELOGS_USER_ID] = event[DBc.FIELD_LOG_USER_ID]
    click[DBc.FIELD_DENSELOGS_TYPE] = event[DBc.FIELD_LOG_TYPE]
    click.update(event[DBc.FIELD_LOG_METAINFO])

    video_id = event[DBc.FIELD_LOG_VIDEO_ID]
    course_id = event[DBc.FIELD_LOG_COURSE_ID]
    event_date = round_timestamp_to_day(event[DBc.FIELD_LOG_TIMESTAMP])
    denselogs_key = "{0}_{1}_{2}".format(course_id, video_id, event_date)

    expected = {
        DBc.FIELD_DENSELOGS_COURSE_ID: course_id,
        DBc.FIELD_DENSELOGS_TIMESTAMP: event_date,
        DBc.FIELD_DENSELOGS_VIDEO_ID: video_id,
        DBc.FIELD_DENSELOGS_CLICKS: [click] * count
    }

    return {denselogs_key: expected}

def get_video_expected_return(hotness):
    date = try_parse_date("2014-08-23T04:57:33.502967+00:00", "%Y-%m-%dT%H:%M:%S.%f+00:00")
    video = {'17df731f26364049908a8c227cb4c3c3': {DBc.FIELD_VIDEO_TEMPORAL_HOTNESS: {}}}
    if isinstance(hotness, int):
        video['17df731f26364049908a8c227cb4c3c3'][
            DBc.FIELD_VIDEO_TEMPORAL_HOTNESS][round_timestamp_to_day(date)] = hotness
    return video

class TestLogProcessor(unittest.TestCase):

    def test_constructor(self):
        '''test the constructor
        '''
        processor = LogProcessor()
        self.assertIsInstance(processor, PipeModule, 'it should be a subclass of PipeModule')
        self.assertEqual(processor.sql_table, 'click_events',
                         'the target table name should be `click_events`')
        self.assertListEqual(processor.events, [], 'the events should be empty')
        self.assertDictEqual(processor.denselogs, {}, 'the denselogs should be empty')

    @patch('mathematician.Processor.Sql2MongoProcessor.log.get_data_by_table')
    def test_load_data(self, mock_get_data_by_table):
        result = 'asdf'
        mock_get_data_by_table.return_value = result
        processor = LogProcessor()
        self.assertIs(processor.load_data(), result, 'should return data from get_data_by_table')

    @patch('mathematician.Processor.Sql2MongoProcessor.log.LogProcessor.load_data')
    def test_process(self, mock_load_data):
        mock_load_data.return_value = None

        # - load nothing
        processor = LogProcessor()
        raw_data = None
        self.assertIs(processor.process(raw_data), raw_data,
                      'should return raw_data if load_data return None')

        log_count = 10
        mock_load_data.return_value = [[
            '_id',  # 0
            'org:HKUSTx/COMP102x/2T2014',  # 1 course_id
            4540165,  # 2 user_id
            '17df731f26364049908a8c227cb4c3c3',  # 3 video_id
            try_parse_date("2014-08-23T04:57:33.502967+00:00", "%Y-%m-%dT%H:%M:%S.%f+00:00"),  # 4 timestamp
            'play_video',  # 5 log_type
            '/path',  # 6
            '', # 7
            79,  # 8 currentTime
            1,  # 9 newTIme
            1,  # 10 oldTime
            2,  # 11 newSpeed
            2,  # 12 oldSpeed
        ]] * log_count

        # - generate log and dense log
        processor = LogProcessor()
        raw_data = {'data': {}}
        processed_data = processor.process(raw_data)
        expectd_log = get_log_expected_return()
        expectd_denselog = get_denselog_expected_return(
            expectd_log, log_count)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * log_count,
            DBc.COLLECTION_DENSELOGS: list(expectd_denselog.values())
        }}
        self.maxDiff = None
        self.assertDictEqual(processed_data, expected, 'should generate logs and denselogs')

        # - augment the exist dense log
        raw_data = {'data': {}}
        processed_data = processor.process(raw_data)
        expectd_log = get_log_expected_return()
        expectd_denselog = get_denselog_expected_return(expectd_log, log_count * 2)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * log_count * 2,
            DBc.COLLECTION_DENSELOGS: list(expectd_denselog.values())
        }}
        self.assertDictEqual(processed_data, expected, 'should augment exist logs and denselogs')

        # - do nothing if no videos
        processor = LogProcessor()
        raw_data = {'data': {
            DBc.COLLECTION_VIDEO: {}
        }}
        processed_data = processor.process(raw_data)
        expectd_log = get_log_expected_return()
        expectd_denselog = get_denselog_expected_return(
            expectd_log, log_count)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * log_count,
            DBc.COLLECTION_DENSELOGS: list(expectd_denselog.values()),
            DBc.COLLECTION_VIDEO: {}
        }}
        self.assertDictEqual(processed_data, expected,
                             'should not modify the videos if no target video in it')

        # - should do nothing if no temprohotness field
        processor = LogProcessor()
        raw_data = {'data': {
            DBc.COLLECTION_VIDEO: {'17df731f26364049908a8c227cb4c3c3': {}}
        }}
        processed_data = processor.process(raw_data)
        expectd_log = get_log_expected_return()
        expectd_denselog = get_denselog_expected_return(expectd_log, log_count)
        expectd_video = get_video_expected_return(log_count)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * log_count,
            DBc.COLLECTION_DENSELOGS: list(expectd_denselog.values()),
            DBc.COLLECTION_VIDEO: expectd_video
        }}
        self.assertDictEqual(processed_data, expected,
                             'should auto add temporalhotness if no temporalhotness field in target video')

        # - augment video if video does not contain temporalHotness
        processor = LogProcessor()
        raw_data = {'data': {
            DBc.COLLECTION_VIDEO: get_video_expected_return(None)
        }}
        processed_data = processor.process(raw_data)
        expectd_log = get_log_expected_return()
        expectd_denselog = get_denselog_expected_return(
            expectd_log, log_count)
        expectd_videos = get_video_expected_return(log_count)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * log_count,
            DBc.COLLECTION_DENSELOGS: list(expectd_denselog.values()),
            DBc.COLLECTION_VIDEO: expectd_videos,
        }}
        self.assertDictEqual(processed_data, expected, 'should init the temproal hotness of video')

        # - augment video if contains temporalHotness
        processor = LogProcessor()
        temproal_hotness = 3
        raw_data = {'data': {
            DBc.COLLECTION_VIDEO: get_video_expected_return(temproal_hotness)
        }}
        processed_data = processor.process(raw_data)
        expectd_log = get_log_expected_return()
        expectd_denselog = get_denselog_expected_return(expectd_log, log_count)
        expectd_videos = get_video_expected_return(temproal_hotness + log_count)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * log_count,
            DBc.COLLECTION_DENSELOGS: list(expectd_denselog.values()),
            DBc.COLLECTION_VIDEO: expectd_videos,
        }}
        self.assertDictEqual(processed_data, expected, 'should update the temproalHotness of video')
