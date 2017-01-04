'''Unit test for Config module
'''
# pylint: disable=C0111, C0103
import unittest
from unittest.mock import patch, MagicMock, mock_open
from logging import INFO
from mathematician.config import init_config

class TestConfig(unittest.TestCase):

    def test_init_config_with_wrong_params(self):
        with self.assertLogs("vismooc", level=INFO) as cm:
            init_config("asdfs")
        self.assertEqual(cm.output, ["WARNING:vismooc:The config file does not exist"])

    @patch("os.path.exists")
    def test_init_config_with_wrong_json_file(self, mock_path):
        mock_path.return_value = True
        with patch('__main__.open', mock_open(read_data='bibble')) as m:
            init_config("foo")
