'''Unit test for Config module
'''
# pylint: disable=C0111, C0103
import unittest
from unittest.mock import patch, MagicMock
from logging import INFO
from mathematician.config import init_config

class TestConfig(unittest.TestCase):

    def test_init_config_with_wrong_params(self):
        with self.assertLogs("vismooc", level=INFO) as cm:
            init_config("asdfs")
        self.assertEqual(cm.output, ["WARNING:vismooc:The config file does not exist"])

    @patch("mathematician.config.exists")
    @patch("mathematician.config.open")
    def test_init_config_with_wrong_json_file(self, mock_exists, mock_open):
        mock_exists.return_value = True
        open_cm = MagicMock()
        open_cm.__enter__.return_value = open_cm
        open_cm.return_value = "balabala"
        mock_open.return_value = open_cm
        print(open_cm.__exit__())
        init_config("foo")
