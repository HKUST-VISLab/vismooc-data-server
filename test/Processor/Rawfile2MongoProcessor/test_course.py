'''Unit test for CourseProcessor
'''
# pylint: disable=C0111, C0103
import unittest
# from unittest.mock import DEFAULT, MagicMock, patch

from mathematician.pipe import PipeModule
from mathematician.Processor.Rawfile2MongoProcessor import course

class TestCourseProcessor(unittest.TestCase):

    def test_constructor(self):
        processor = course.CourseProcessor()
        self.assertIsInstance(processor, PipeModule, 'it should be a subclass of PipeModule')
        self.assertDictEqual(processor.videos, {}, 'the videos should be empty')
        self.assertDictEqual(processor.courses, {}, 'the courses should be empty')
        self.assertListEqual(processor.processed_files, [], 'the processed_files should be empty')
