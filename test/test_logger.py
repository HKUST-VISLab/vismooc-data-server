'''Unit test for Logger module
'''
# pylint: disable=C0111, C0103
import unittest
from logging import INFO
from mathematician import logger, __version__


class TestLogger(unittest.TestCase):

    def test_warn(self):
        msg = "a test msg"
        with self.assertLogs("vismooc-ds@" + __version__, level=INFO) as cm:
            logger.warn(msg)
        self.assertEqual(
            cm.output, ["WARNING:vismooc-ds@" + __version__ + ":" + msg])

    def test_info(self):
        msg = "a test msg"
        with self.assertLogs("vismooc-ds@" + __version__, level=INFO) as cm:
            logger.info(msg)
        self.assertEqual(
            cm.output, ["INFO:vismooc-ds@" + __version__ + ":" + msg])
