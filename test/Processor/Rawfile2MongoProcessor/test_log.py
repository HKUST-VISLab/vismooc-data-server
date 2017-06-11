'''Unit test for LogProcessor
'''
# pylint: disable=C0111, C0103
import unittest
from unittest.mock import patch, DEFAULT, MagicMock
from datetime import datetime

from mathematician.pipe import PipeModule
from mathematician.Processor.Rawfile2MongoProcessor import LogProcessor
from mathematician.config import DBConfig as DBc
from mathematician.Processor.utils import PARALLEL_GRAIN


class TestLogProcessor(unittest.TestCase):

    def test_constructor(self):
        '''test the constructor
        '''
        processor = LogProcessor()
        self.assertIsInstance(processor, PipeModule,
                              'it should be a subclass of PipeModule')
        self.assertListEqual(processor.processed_files, [],
                             'the processed_files should be empty')
        self.assertListEqual(processor.events, [],
                             'the events should be empty')
        self.assertDictEqual(processor.denselogs, {},
                             'the denselogs should be empty')

    @patch.multiple('mathematician.Processor.Rawfile2MongoProcessor.log', open=DEFAULT, is_processed=DEFAULT)
    def test_load_data(self, **mocks):
        mock_open, mock_is_processed = mocks['open'], mocks['is_processed']
        open_cm = MagicMock()
        open_cm.__enter__.return_value = open_cm
        open_cm.readlines.return_value = '111'
        mock_open.return_value = open_cm
        mock_is_processed.return_value = False

        filenames = ['1-clickstream', '2-clickstream',
                     '3-asdf', '4-clickstreamasdf']

        processor = LogProcessor()
        count = 0
        for data in processor.load_data(filenames):
            self.assertEqual(data, '111', 'should read the data from files')
            count += 1
        self.assertIs(
            count, 3, 'should ignore files that do not contains `-clickstream` in file name')

        mock_is_processed.return_value = True
        count = 0
        for data in processor.load_data(filenames):
            self.assertEqual(data, '111', 'should read the data from files')
            count += 1
        self.assertIs(count, 0, 'should ignore files that has been processed')

    def test_is_target_log(self):
        # pylint: disable=C0301
        processor = LogProcessor()
        self.assertIsNone(processor.is_target_log(
            123), msg='should return None if input is not str')

        wrong_event_source = r'{"username": "asdf", "event_type": "pause_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "asdf", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(
            wrong_event_source), msg='should return None if the source_event is wrong')

        wrong_event_type = r'{"username": "samast", "event_type": "asdf", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(
            wrong_event_type), msg='should return None if the event_type is wrong')

        empty_username = r'{"username": "", "event_type": "load_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(
            empty_username), msg='should return None if username is empty')

        no_username = r'{"event_type": "load_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(no_username),
                          msg='should return None if no username')

        no_context = r'{"username": "samast", "event_type": "play_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(no_context),
                          msg='should return None if no context')

        no_timestamp = r'{"username": "asd", "event_type": "load_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}}'
        self.assertIsNone(processor.is_target_log(
            no_timestamp), msg='should return None if no time')

        no_evnet_field = r'{"username": "samast", "event_type": "play_video", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'
        self.assertIsNone(processor.is_target_log(
            no_evnet_field), msg='should return None if no event field')

        # 6 right event_type(1 wrong user, 1 wrong event_source, 1 wrong user and wrong event_source)
        # 1 wrong event_type
        lines = [
            r'{"username": "asd", "event_type": "load_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}',
            r'{"username": "asfd", "event_type": "pause_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}',
            r'{"username": "samast", "event_type": "play_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}',
            r'{"username": "samast", "event_type": "seek_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}',
            r'{"username": "samast", "event_type": "speed_change_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}',
            r'{"username": "samast", "event_type": "stop_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}'
        ]

        for line in lines:
            temp_data = processor.is_target_log(line)
            self.assertIsInstance(
                temp_data, list, msg='the return should be a list')

    def test_process_few_logs(self):
        processor = LogProcessor()
        # pylint: disable=C0301
        no_video_id = [r'{"username": "samast", "event_type": "play_video", "event": "{\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}']
        self.assertListEqual(processor.process_few_logs(
            no_video_id), [], 'should ignore the log without video id')

        wrong_time_format = [r'{"username": "samast", "event_type": "play_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33+00:00"}']
        events = processor.process_few_logs(wrong_time_format)
        self.assertIs(len(events), 1, 'should auto-fix the time format')
        self.assertEqual(events[0][DBc.FIELD_LOG_TIMESTAMP], datetime.strptime(
            "2014-08-23T04:57:33.000000+00:00", LogProcessor.pattern_time).timestamp(), 'should auto-fix the time format with 000000')

        wrong_course_id_format = [r'{"username": "samast", "event_type": "play_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "14x//:HKUSTx+COMP102x+2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33+00:00"}']
        events = processor.process_few_logs(wrong_course_id_format)
        self.assertIs(len(events), 1, 'should auto-fix the course Id format')
        self.assertEqual(events[0][DBc.FIELD_LOG_COURSE_ID], 'HKUSTx_COMP102x_2T2014',
                         'should auto fix the course id with underscore')

        right_log = [r'{"username": "samast", "event_type": "play_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"new_time\":1,\"old_time\":1,\"new_speed\":2,\"old_speed\":2,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}']
        events = processor.process_few_logs(right_log)
        expectd = {}
        expectd[DBc.FIELD_LOG_USER_ID] = 4540165
        expectd[DBc.FIELD_LOG_VIDEO_ID] = '17df731f26364049908a8c227cb4c3c3'
        expectd[DBc.FIELD_LOG_COURSE_ID] = 'HKUSTx_COMP102x_2T2014'
        expectd[DBc.FIELD_LOG_TIMESTAMP] = datetime.strptime(
            "2014-08-23T04:57:33.502967+00:00", LogProcessor.pattern_time).timestamp()
        expectd[DBc.FIELD_LOG_TYPE] = "play_video"
        expectd[DBc.FIELD_LOG_METAINFO] = {
            "currentTime": 79,
            "newTime": 1,
            "oldTime": 1,
            "newSpeed": 2,
            "oldSpeed": 2
        }
        self.assertIs(len(events), 1, 'should parse the log')
        self.assertDictEqual(events[0], expectd,
                             'should parse the log and convert the attr format of log metainfo')

    @patch('mathematician.Processor.Rawfile2MongoProcessor.log.LogProcessor.load_data')
    def test_process(self, mock_load_data):
        # pylint: disable=C0301
        mock_load_data.return_value = None

        processor = LogProcessor()
        raw_data = None
        self.assertIs(processor.process(raw_data), raw_data,
                      'should return raw_data if load_data return None')

        mock_load_data.return_value = [['' for i in range(PARALLEL_GRAIN)]]
        raw_data = {'data': {}}
        expected = {"data": {DBc.COLLECTION_LOG: [],
                             DBc.COLLECTION_DENSELOGS: []}, "processed_files": []}
        processed_data = processor.process(raw_data)
        self.assertDictEqual(processed_data, expected,
                             'should do nothing if no events')

        mock_load_data.return_value = [
            [r'{"username": "samast", "event_type": "play_video", "event": "{\"id\":\"i4x-HKUSTx-COMP102x-video-17df731f26364049908a8c227cb4c3c3\",\"currentTime\":79,\"new_time\":1,\"old_time\":1,\"new_speed\":2,\"old_speed\":2,\"code\":\"z4L-yr1st3I\"}", "event_source": "browser", "context": {"user_id": 4540165, "org_id": "HKUSTx", "course_id": "HKUSTx/COMP102x/2T2014", "path": "/event"}, "time": "2014-08-23T04:57:33.502967+00:00"}' for i in range(PARALLEL_GRAIN)]]
