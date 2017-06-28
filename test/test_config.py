'''Unit test for Config module
'''
# pylint: disable=C0111, C0103
import unittest
from logging import INFO
from unittest.mock import DEFAULT, MagicMock, patch

from mathematician.config import DBConfig as DBc
from mathematician.config import ThirdPartyKeys as TPK
from mathematician.config import init_config

class TestConfig(unittest.TestCase):

    def test_init_config_with_wrong_params(self):
        with self.assertLogs("vismooc", level=INFO) as cm:
            init_config("asdfs")
        self.assertEqual(cm.output, ["WARNING:vismooc:The config file does not exist"])

    @patch.multiple('mathematician.config', exists=DEFAULT, open=DEFAULT)
    def test_init_config_with_default_value(self, **mocks):
        mock_exists, mock_open = mocks["exists"], mocks["open"]
        mock_exists.return_value = True
        open_cm = MagicMock()
        open_cm.__enter__.return_value = open_cm
        open_cm.read.return_value = '{ \
            "data_server": {\
                "third_party_keys": {\
                    "Youtube_key": "test"\
                },\
            }\
        }'
        mock_open.return_value = open_cm

        self.assertIsNone(init_config("foo"))
        target_fields = [DBc.DB_HOST, DBc.DB_NAME, DBc.DB_PORT]
        expect_results = ["mongo_host", "DB_name", "mongo_port"]
        self.assertEqual(target_fields, expect_results)

    @patch.multiple('mathematician.config', exists=DEFAULT, open=DEFAULT)
    def test_init_config_with_wrong_json_file(self, **mocks):
        mock_exists, mock_open = mocks["exists"], mocks["open"]
        mock_exists.return_value = True
        open_cm = MagicMock()
        open_cm.__enter__.return_value = open_cm
        open_cm.read.return_value = "{balabala"
        mock_open.return_value = open_cm

        first_warn = "WARNING:vismooc:Decode init json file failed"
        second_warn = "WARNING:vismooc:Expecting property name enclosed in double quotes"
        with self.assertLogs("vismooc", level=INFO) as cm:
            self.assertIsNone(init_config("foo"))
        self.assertEqual(cm.output, [first_warn, second_warn])

    @patch.multiple('mathematician.config', exists=DEFAULT, open=DEFAULT)
    def test_init_config_with_right_json_file(self, **mocks):
        mock_exists, mock_open = mocks["exists"], mocks["open"]
        mock_exists.return_value = True
        open_cm = MagicMock()
        open_cm.__enter__.return_value = open_cm
        open_cm.read.return_value = '{\
            "mongo": {\
                "host": "test",\
                "name": "test",\
                "port": "test"\
            },\
            "data_server": {\
                "third_party_keys": {\
                    "Youtube_key": "test"\
                }\
            }\
        }'
        mock_open.return_value = open_cm

        self.assertIsNone(init_config("foo"))
        test_value = "test"
        target_fields = [DBc.DB_HOST, DBc.DB_NAME, TPK.Youtube_key]
        expect_results = len(target_fields) * [test_value]
        target_fields.append(DBc.DB_PORT)
        expect_results.append(1123)
        self.assertEqual(target_fields, expect_results)
