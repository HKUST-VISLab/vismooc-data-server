'''Unit test for LogProcessor
'''
# pylint: disable=C0111, C0103
import unittest
from unittest.mock import patch, DEFAULT, MagicMock

from mathematician.pipe import PipeModule
from mathematician.Processor.HKMOOC2MongoProcessor import LogProcessor
from mathematician.config import FilenameConfig as FC
from mathematician.config import DBConfig as DBc
from mathematician.Processor.utils import PARALLEL_GRAIN, round_timestamp_to_day, try_parse_date, try_get_timestamp


def get_right_log(event_type='play_video'):
    # pylint: disable=C0301
    return r'{"username": "samast", "event_type": "' + event_type + r'", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"new_time\":1,\"old_time\":1,\"new_speed\":2,\"old_speed\":2,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "org:HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'


def get_log_expected_return(event_type='play_video'):
    expectd = {}
    expectd[DBc.FIELD_LOG_USER_ID] = 4540165
    expectd[DBc.FIELD_LOG_VIDEO_ID] = '17df731f26364049908a8c227cb4c3c3'
    expectd[DBc.FIELD_LOG_COURSE_ID] = 'HKUSTx_COMP102x_2T2014'
    expectd[DBc.FIELD_LOG_TIMESTAMP] = try_get_timestamp(try_parse_date("2014-08-23T04:57:33.502967+00:00",
                                                                        LogProcessor.pattern_date))
    expectd[DBc.FIELD_LOG_TYPE] = event_type
    expectd[DBc.FIELD_LOG_METAINFO] = {
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

    course_id = event[DBc.FIELD_LOG_COURSE_ID]
    video_id = event[DBc.FIELD_LOG_VIDEO_ID]
    event_date = round_timestamp_to_day(event[DBc.FIELD_LOG_TIMESTAMP])
    denselogs_key = "{0}_{1}_{2}".format(course_id, video_id, event_date)

    expected = {
        DBc.FIELD_DENSELOGS_COURSE_ID: course_id,
        DBc.FIELD_DENSELOGS_VIDEO_ID: video_id,
        DBc.FIELD_DENSELOGS_TIMESTAMP: event_date,
        DBc.FIELD_DENSELOGS_CLICKS: [click] * count
    }

    return {denselogs_key: expected}


def get_video_expected_return(hotness):
    date = try_parse_date("2014-08-23T04:57:33.502967+00:00", LogProcessor.pattern_date)
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
        self.assertIsInstance(processor, PipeModule,
                              'it should be a subclass of PipeModule')

    @patch.multiple('mathematician.Processor.HKMOOC2MongoProcessor.log', open=DEFAULT)
    def test_load_data(self, **mocks):
        mock_open = mocks['open']
        open_cm = MagicMock()
        open_cm.__enter__.return_value = open_cm
        open_cm.readlines.return_value = '111'
        mock_open.return_value = open_cm

        processor = LogProcessor()
        filenames = [None, 'asdf', {'path': '1-' + FC.Clickstream_suffix}, {'path': '2-' + FC.Clickstream_suffix},
                     {'path': '3-asdf'}, {'path': '4-' + FC.Clickstream_suffix}]
        count = 0
        for data in processor.load_data(filenames):
            self.assertEqual(data, '111', 'should read the data from files')
            count += 1
        self.assertIs(
            count, 3, 'should ignore files that do not contains `-clickstream` in file name')

    def test_is_target_log(self):
        # pylint: disable=C0301
        processor = LogProcessor()
        self.assertIsNone(processor.is_target_log(
            123), msg='should return None if input is not str')

        wrong_event_source = r'{"username": "asdf", "event_type": "pause_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "asdf", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(
            wrong_event_source), msg='should return None if the source_event is wrong')

        wrong_event_type = r'{"username": "samast", "event_type": "asdf", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "org:HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(
            wrong_event_type), msg='should return None if the event_type is wrong')

        empty_username = r'{"username": "", "event_type": "pause_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "org:HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(
            empty_username), msg='should return None if username is empty')

        no_username = r'{"event_type": "pause_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "org:HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(no_username),
                          msg='should return None if no username')

        no_context = r'{"username": "samast", "event_type": "play_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(no_context),
                          msg='should return None if no context')

        no_timestamp = r'{"username": "asd", "event_type": "pause_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "org:HKUSTx/COMP102x/2T2014", "path": "/event"}}'
        self.assertIsNone(processor.is_target_log(
            no_timestamp), msg='should return None if no time')

        no_evnet_field = r'{"username": "samast", "event_type": "play_video", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "org:HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(
            no_evnet_field), msg='should return None if no event field')

        # 6 right event_type
        target_event_type = ['pause_video', 'play_video',
                             'seek_video', 'speed_change_video', 'stop_video']
        lines = [get_right_log(event_type) for event_type in target_event_type]

        for line in lines:
            temp_data = processor.is_target_log(line)
            self.assertIsInstance(temp_data, list, msg='the return should be a list')

    def test_process_few_logs(self):
        processor = LogProcessor()
        # pylint: disable=C0301
        no_video_id = [r'{"username": "samast", "event_type": "play_video", "event": "{\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "org:HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}']
        self.assertListEqual(processor.process_few_logs(
            no_video_id), [], 'should ignore the log without video id')

        wrong_time_format = [r'{"username": "samast", "event_type": "play_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "org:HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33+00:00"}']
        events = processor.process_few_logs(wrong_time_format)
        self.assertIs(len(events), 1, 'should auto-fix the time format')
        self.assertEqual(events[0][DBc.FIELD_LOG_TIMESTAMP], try_get_timestamp(try_parse_date(
            "2014-08-23T04:57:33.000000+00:00", LogProcessor.pattern_date)), 'should auto-fix the time format with 000000')

        wrong_course_id_format = [r'{"username": "samast", "event_type": "play_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "14x//:HKUSTx+COMP102x+2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33+00:00"}']
        events = processor.process_few_logs(wrong_course_id_format)
        self.assertIs(len(events), 1, 'should auto-fix the course Id format')
        self.assertEqual(events[0][DBc.FIELD_LOG_COURSE_ID], 'HKUSTx_COMP102x_2T2014',
                         'should auto fix the course id with underscore')

        right_log = [get_right_log()]
        events = processor.process_few_logs(right_log)
        expectd = get_log_expected_return()
        self.assertIs(len(events), 1, 'should parse the log')
        self.assertDictEqual(events[0], expectd,
                             'should parse the log and convert the attr format of log metainfo')

    @patch('mathematician.Processor.HKMOOC2MongoProcessor.log.LogProcessor.load_data')
    def test_process(self, mock_load_data):
        # pylint: disable=C0301
        mock_load_data.return_value = None

        # - load nothing
        processor = LogProcessor()
        raw_data = None
        self.assertIs(processor.process(raw_data), raw_data,
                      'should return raw_data if load_data return None')

        # - input empty logs
        mock_load_data.return_value = [['' for i in range(PARALLEL_GRAIN)]]
        raw_data = {'data': {}}
        processed_data = processor.process(raw_data)
        expected = {"data": {
            DBc.COLLECTION_LOG: [],
            DBc.COLLECTION_DENSELOGS: []
        }}
        self.assertDictEqual(processed_data, expected, 'should do nothing if no events')

        mock_load_data.return_value = [[get_right_log()] * PARALLEL_GRAIN]

        # - generate log and dense log
        processor = LogProcessor()
        raw_data = {'data': {}}
        processed_data = processor.process(raw_data)
        expectd_log = get_log_expected_return()
        expectd_denselog = get_denselog_expected_return(expectd_log, PARALLEL_GRAIN)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * PARALLEL_GRAIN,
            DBc.COLLECTION_DENSELOGS: list(expectd_denselog.values())
        }}
        self.assertDictEqual(processed_data, expected, 'should generate logs and denselogs')

        # - augment the exist dense log
        raw_data = {'data': {}}
        processed_data = processor.process(raw_data)
        expectd_log = get_log_expected_return()
        expectd_denselog = get_denselog_expected_return(expectd_log, PARALLEL_GRAIN * 2)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * PARALLEL_GRAIN * 2,
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
            expectd_log, PARALLEL_GRAIN)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * PARALLEL_GRAIN,
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
        expectd_denselog = get_denselog_expected_return(
            expectd_log, PARALLEL_GRAIN)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * PARALLEL_GRAIN,
            DBc.COLLECTION_DENSELOGS: list(expectd_denselog.values()),
            DBc.COLLECTION_VIDEO: get_video_expected_return(PARALLEL_GRAIN)
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
        expectd_denselog = get_denselog_expected_return(expectd_log, PARALLEL_GRAIN)
        expectd_videos = get_video_expected_return(PARALLEL_GRAIN)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * PARALLEL_GRAIN,
            DBc.COLLECTION_DENSELOGS: list(expectd_denselog.values()),
            DBc.COLLECTION_VIDEO: expectd_videos,
        }}
        self.assertDictEqual(processed_data, expected,
                             'should init the temproal hotness of video')

        # - augment video if contains temporalHotness
        processor = LogProcessor()
        temproal_hotness = 3
        raw_data = {'data': {
            DBc.COLLECTION_VIDEO: get_video_expected_return(temproal_hotness)
        }}
        processed_data = processor.process(raw_data)
        expectd_log = get_log_expected_return()
        expectd_denselog = get_denselog_expected_return(
            expectd_log, PARALLEL_GRAIN)
        expectd_videos = get_video_expected_return(
            temproal_hotness + PARALLEL_GRAIN)
        expected = {"data": {
            DBc.COLLECTION_LOG: [expectd_log] * PARALLEL_GRAIN,
            DBc.COLLECTION_DENSELOGS: list(expectd_denselog.values()),
            DBc.COLLECTION_VIDEO: expectd_videos,
        }}
        self.assertDictEqual(processed_data, expected,
                             'should update the temproal hotness of video')
