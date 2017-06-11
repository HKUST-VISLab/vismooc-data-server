'''Unit test for Logger module
'''
# pylint: disable=C0111, C0103
import unittest
from logging import INFO
from mathematician import logger

class TestLogger(unittest.TestCase):

    def test_warn(self):
        msg = "a test msg"
        with self.assertLogs("vismooc", level=INFO) as cm:
            logger.warn(msg)
        self.assertEqual(cm.output, ["WARNING:vismooc:"+msg])

    def test_info(self):
        msg = "a test msg"
        with self.assertLogs("vismooc", level=INFO) as cm:
            logger.info(msg)
        self.assertEqual(cm.output, ["INFO:vismooc:"+msg])
    