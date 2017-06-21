'''Unit test for LogProcessor
'''
# pylint: disable=C0111, C0103
import unittest
from unittest.mock import patch, MagicMock

from mathematician.pipe import PipeModule
from mathematician.Processor.Rawfile2MongoProcessor import EnrollmentProcessor
from mathematician.Processor.Rawfile2MongoProcessor.constant import RD_DATA
from mathematician.Processor.utils import try_get_timestamp, try_parse_date
from mathematician.config import DBConfig as DBc


def get_enrollment_data():
    return [
        'id\tuser_id\tcourse_id\tcreated\tis_active\tmode\n',
        '5525661\t1359538\tHKUSTx/COMP102x/2T2014\t2014-03-28 13:19:05\t1\thonor\n',
        '5686086\t133068\torg:HKUSTx/COMP102x/2T2014\t2014-04-10 14:58:33\t1\thonor\n',
        '5686102\t1503175\tHKUSTx/COMP102x/2T2014\t2014-04-10 14:59:55\t0\thonor\n',
        '5686102\t1503175\tHKUSTx/COMP102x/2T2014\t2014-04-10 14:59:55\t1\thonor\n',
        '5686102\t1503175\tHKUSTx/COMP102x/2T2014\t2014-04-10 14:59:55\t0\thonor\n',
    ]


class TestEnrollmentProcessor(unittest.TestCase):

    def test_constructor(self):
        '''test the constructor
        '''
        processor = EnrollmentProcessor()
        self.assertIsInstance(processor, PipeModule, 'it should be a subclass of PipeModule')
        self.assertListEqual(processor.processed_files, [], 'the processed_files should be empty')
        self.assertListEqual(processor.enrollments, [], 'the enrollments should be empty')

    @patch('mathematician.Processor.Rawfile2MongoProcessor.enrollment.open')
    def test_load_data(self, mock_open):
        raw_data = get_enrollment_data()
        # read_data = iter(raw_data)

        open_cm = MagicMock()

        def __enter__(self):
            self.read_data = iter(raw_data)
            return open_cm
        open_cm.__enter__ = __enter__
        open_cm.__next__ = lambda self: next(self.read_data)
        open_cm.readlines = lambda: list(open_cm.read_data)
        mock_open.return_value = open_cm

        data_filenames = ['1-student_courseenrollment-', '2-student_courseenrollment-asdf', 'asdf']
        processor = EnrollmentProcessor()
        target_data = processor.load_data(data_filenames)
        count = 0
        for data in target_data:
            self.assertEqual(data, raw_data[1:], 'the header should be remove and return raw_data')
            count += 1
        self.assertIs(count, 2, 'should ignore files without `-student_courseenrollment-` in filename')
        self.assertListEqual(processor.processed_files, data_filenames[:-1], 'should recorded the file processed')

    @patch('mathematician.Processor.Rawfile2MongoProcessor.enrollment.EnrollmentProcessor.load_data')
    def test_process(self, mock_load_data):
        # pylint: disable=C0301
        mock_load_data.return_value = 123
        # - nothing to load
        processor = EnrollmentProcessor()
        raw_data = 123
        raw_data_filenames = 'asdf'
        self.assertIs(processor.process(raw_data), raw_data,
                      'should return raw_data if raw_data_filename is None')

        mock_load_data.return_value = None
        # -load nothing
        raw_data = 123
        self.assertIs(processor.process(raw_data, raw_data_filenames), raw_data,
                      'should return raw_data if load_data return None')

        mock_load_data.return_value = [get_enrollment_data()]
        # - raise exception if no courses table
        raw_data = {RD_DATA: {}}
        processor = EnrollmentProcessor()
        with self.assertRaises(Exception, msg="should raise exception if no courses table in raw_data"):
            processor.process(raw_data, raw_data_filenames)

        raw_data = {RD_DATA: {
            DBc.COLLECTION_COURSE: {}
        }}
        with self.assertRaises(Exception, msg="should raise exception if no users table in raw_data"):
            processor.process(raw_data, raw_data_filenames)

        # no user exists
        raw_data = {RD_DATA: {
            DBc.COLLECTION_USER: {},
            DBc.COLLECTION_COURSE: {
                'HKUSTx_COMP102x_2T2014': {
                    DBc.FIELD_COURSE_STUDENT_IDS: set()
                }
            }
        }}
        processor = EnrollmentProcessor()
        processed_data = processor.process(raw_data, raw_data_filenames)
        excepted = {
            RD_DATA: {
                DBc.COLLECTION_ENROLLMENT: [],
                DBc.COLLECTION_USER: {},
                DBc.COLLECTION_COURSE: {
                    'HKUSTx_COMP102x_2T2014': {
                        DBc.FIELD_COURSE_STUDENT_IDS: set()
                    }
                }
            },
            'processed_files': []
        }
        self.assertDictEqual(processed_data, excepted, 'should generate no enrollment records if no users')

        # no exist course
        raw_data = {RD_DATA: {
            DBc.COLLECTION_USER: {
                '1359538': {
                    DBc.FIELD_USER_COURSE_IDS: set(),
                    DBc.FIELD_USER_DROPPED_COURSE_IDS: set()
                },
                '133068': {
                    DBc.FIELD_USER_COURSE_IDS: set(),
                    DBc.FIELD_USER_DROPPED_COURSE_IDS: set()
                },
                '1503175': {
                    DBc.FIELD_USER_COURSE_IDS: set(),
                    DBc.FIELD_USER_DROPPED_COURSE_IDS: set()
                }
            },
            DBc.COLLECTION_COURSE: {}
        }}
        processor = EnrollmentProcessor()
        processed_data = processor.process(raw_data, raw_data_filenames)
        excepted = {
            RD_DATA: {
                DBc.COLLECTION_ENROLLMENT: [],
                DBc.COLLECTION_USER: {
                    '1359538': {
                        DBc.FIELD_USER_COURSE_IDS: set(),
                        DBc.FIELD_USER_DROPPED_COURSE_IDS: set()
                    },
                    '133068': {
                        DBc.FIELD_USER_COURSE_IDS: set(),
                        DBc.FIELD_USER_DROPPED_COURSE_IDS: set()
                    },
                    '1503175': {
                        DBc.FIELD_USER_COURSE_IDS: set(),
                        DBc.FIELD_USER_DROPPED_COURSE_IDS: set()
                    }
                },
                DBc.COLLECTION_COURSE: {}
            },
            'processed_files': []
        }
        self.assertDictEqual(processed_data, excepted, 'should generate no enrollment records if no courses')

        # have course and users, processe different course_id
        raw_data = {RD_DATA: {
            DBc.COLLECTION_USER: {
                '1359538': {
                    DBc.FIELD_USER_COURSE_IDS: set(),
                    DBc.FIELD_USER_DROPPED_COURSE_IDS: set()
                },
                '133068': {
                    DBc.FIELD_USER_COURSE_IDS: set(),
                    DBc.FIELD_USER_DROPPED_COURSE_IDS: set()
                }
            },
            DBc.COLLECTION_COURSE: {
                'HKUSTx_COMP102x_2T2014': {
                    DBc.FIELD_COURSE_STUDENT_IDS: set()
                }
            }
        }}
        processor = EnrollmentProcessor()
        processed_data = processor.process(raw_data, raw_data_filenames)
        excepted_course_id = 'HKUSTx_COMP102x_2T2014'
        excepted_user_id_1 = '1359538'
        excepted_enrollment_1 = {
            DBc.FIELD_ENROLLMENT_COURSE_ID: excepted_course_id,
            DBc.FIELD_ENROLLMENT_USER_ID: excepted_user_id_1,
            DBc.FIELD_ENROLLMENT_TIMESTAMP: try_get_timestamp(
                try_parse_date('2014-03-28 13:19:05', EnrollmentProcessor.pattern_date)),
            DBc.FIELD_ENROLLMENT_ACTION: EnrollmentProcessor.action.get('1')
        }
        excepted_user_id_2 = '133068'
        excepted_enrollment_2 = {
            DBc.FIELD_ENROLLMENT_COURSE_ID: excepted_course_id,
            DBc.FIELD_ENROLLMENT_USER_ID: excepted_user_id_2,
            DBc.FIELD_ENROLLMENT_TIMESTAMP: try_get_timestamp(
                try_parse_date('2014-04-10 14:58:33', EnrollmentProcessor.pattern_date)),
            DBc.FIELD_ENROLLMENT_ACTION: EnrollmentProcessor.action.get('1')
        }

        excepted = {
            RD_DATA: {
                DBc.COLLECTION_ENROLLMENT: [excepted_enrollment_1, excepted_enrollment_2],
                DBc.COLLECTION_USER: {
                    excepted_user_id_1: {
                        DBc.FIELD_USER_COURSE_IDS: set([excepted_course_id]),
                        DBc.FIELD_USER_DROPPED_COURSE_IDS: set()
                    },
                    excepted_user_id_2: {
                        DBc.FIELD_USER_COURSE_IDS: set([excepted_course_id]),
                        DBc.FIELD_USER_DROPPED_COURSE_IDS: set()
                    }
                },
                DBc.COLLECTION_COURSE: {
                    excepted_course_id: {
                        DBc.FIELD_COURSE_STUDENT_IDS: set([excepted_user_id_1, excepted_user_id_2])
                    }
                }
            },
            'processed_files': []
        }
        self.assertDictEqual(processed_data, excepted,
                             'should generate enrollment records and augment users and courses table')

        # unenroll and enroll and unenroll
        raw_data = {RD_DATA: {
            DBc.COLLECTION_USER: {
                '1503175': {
                    DBc.FIELD_USER_COURSE_IDS: set(),
                    DBc.FIELD_USER_DROPPED_COURSE_IDS: set()
                },
            },
            DBc.COLLECTION_COURSE: {
                'HKUSTx_COMP102x_2T2014': {
                    DBc.FIELD_COURSE_STUDENT_IDS: set()
                }
            }
        }}
        processor = EnrollmentProcessor()
        processed_data = processor.process(raw_data, raw_data_filenames)
        excepted_course_id = 'HKUSTx_COMP102x_2T2014'
        excepted_user_id_1 = '1503175'
        excepted_enrollment_1 = {
            DBc.FIELD_ENROLLMENT_COURSE_ID: excepted_course_id,
            DBc.FIELD_ENROLLMENT_USER_ID: excepted_user_id_1,
            DBc.FIELD_ENROLLMENT_TIMESTAMP: try_get_timestamp(
                try_parse_date('2014-04-10 14:59:55', EnrollmentProcessor.pattern_date)),
            DBc.FIELD_ENROLLMENT_ACTION: EnrollmentProcessor.action.get('0')
        }
        excepted_enrollment_2 = {
            DBc.FIELD_ENROLLMENT_COURSE_ID: excepted_course_id,
            DBc.FIELD_ENROLLMENT_USER_ID: excepted_user_id_1,
            DBc.FIELD_ENROLLMENT_TIMESTAMP: try_get_timestamp(
                try_parse_date('2014-04-10 14:59:55', EnrollmentProcessor.pattern_date)),
            DBc.FIELD_ENROLLMENT_ACTION: EnrollmentProcessor.action.get('1')
        }
        excepted_enrollment_3 = {
            DBc.FIELD_ENROLLMENT_COURSE_ID: excepted_course_id,
            DBc.FIELD_ENROLLMENT_USER_ID: excepted_user_id_1,
            DBc.FIELD_ENROLLMENT_TIMESTAMP: try_get_timestamp(
                try_parse_date('2014-04-10 14:59:55', EnrollmentProcessor.pattern_date)),
            DBc.FIELD_ENROLLMENT_ACTION: EnrollmentProcessor.action.get('0')
        }

        excepted = {
            RD_DATA: {
                DBc.COLLECTION_ENROLLMENT: [excepted_enrollment_1, excepted_enrollment_2, excepted_enrollment_3],
                DBc.COLLECTION_USER: {
                    excepted_user_id_1: {
                        DBc.FIELD_USER_COURSE_IDS: set(),
                        DBc.FIELD_USER_DROPPED_COURSE_IDS: set([excepted_course_id])
                    },
                },
                DBc.COLLECTION_COURSE: {
                    excepted_course_id: {
                        DBc.FIELD_COURSE_STUDENT_IDS: set()
                    }
                }
            },
            'processed_files': []
        }
        self.assertDictEqual(processed_data, excepted,
                             'should generate enrollment records and augment users and courses table')
