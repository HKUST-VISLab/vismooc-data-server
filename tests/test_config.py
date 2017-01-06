'''Unit test for Config module
'''
# pylint: disable=C0111, C0103
import unittest
import json
from unittest.mock import patch, MagicMock, DEFAULT
from logging import INFO
from mathematician.config import init_config

class TestConfig(unittest.TestCase):

    def test_init_config_with_wrong_params(self):
        with self.assertLogs("vismooc", level=INFO) as cm:
            init_config("asdfs")
        self.assertEqual(cm.output, ["WARNING:vismooc:The config file does not exist"])

    # @patch("mathematician.config.exists")
    # @patch("mathematician.config.open")
    @patch.multiple('mathematician.config', exists=DEFAULT, open=DEFAULT)
    def test_init_config_with_wrong_json_file(self, **mocks):
        mock_exists, mock_open = mocks["exists"], mocks["open"]
        mock_exists.return_value = True
        open_cm = MagicMock()
        open_cm.__enter__.return_value = open_cm
        open_cm.return_value = "balabala"
        mock_open.return_value = open_cm
        self.assertRaises(json.JSONDecoder, msg="if config file cannot be parsed as JSON, raise an\
                          exception"):
            init_config("foo")
