'''Unit test for Processor.OldProcessCourseStructureFile
'''
# pylint: disable=C0111, C0103
import unittest
# from unittest.mock import DEFAULT, MagicMock, patch

from mathematician.pipe import PipeModule
from mathematician.Processor.Sql2MongoProcessor.course import CourseProcessor

class TestCourseProcessor(unittest.TestCase):

    def test_constructor(self):
        processor = CourseProcessor()
        self.assertIsInstance(processor, PipeModule, 'it should be a subclass of PipeModule')
        self.assertEqual(processor.sql_table, 'courses', 'the videos should be empty')
        self.assertDictEqual(processor.courses, {}, 'the courses should be empty')
