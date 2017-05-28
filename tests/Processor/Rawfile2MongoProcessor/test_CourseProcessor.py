'''Unit test for Processor.OldProcessCourseStructureFile
'''
# pylint: disable=C0111, C0103
import unittest
from unittest.mock import DEFAULT, MagicMock, patch

from mathematician.pipe import PipeModule
from mathematician.Processor.Rawfile2MongoProcessor import CourseProcessor


class TestProcessCourseStructureFile(unittest.TestCase):

    def test_constructor(self):
        processor = CourseProcessor()
        self.assertIsInstance(processor, PipeModule, 'it should be a subclass of PipeModule')
