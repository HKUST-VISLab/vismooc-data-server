'''Unit test for Config module
'''
# pylint: disable=C0111, C0103
import unittest
from unittest.mock import patch, MagicMock, DEFAULT
from logging import INFO
from mathematician.config import init_config, DataSource as DS, FilenameConfig as FC, ThirdPartyKeys as TPK, DBConfig as DBC


class TestConfig(unittest.TestCase):

    def test_init_config_with_wrong_params(self):
        with self.assertLogs("vismooc", level=INFO) as cm:
            init_config("asdfs")
        self.assertEqual(cm.output, ["WARNING:vismooc:The config file does not exist"])

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
        open_cm.read.return_value = '{ \
            "mongo": {\
                "host": "test",\
                "name": "test",\
                "port": "test"\
            },\
            "data_server": {\
                "third_party_keys": {\
                    "HKMOOC_key": "test",\
                    "Youtube_key": "test"\
                },\
                "data_sources": {\
                    "data_source_host": "test",\
                    "access_tokens_url": "test",\
                    "clickstreams_url": "test",\
                    "mongoDB_url": "test",\
                    "SQLDB_url": "test"\
                },\
                "data_filenames": {\
                    "data_dir":"test",\
                    "mongodb_file": "test",\
                    "sqldb_file": "test",\
                    "meta_db_record": "test",\
                    "active_versions": "test",\
                    "structures": "test"\
                }\
            }\
        }'
        mock_open.return_value = open_cm

        self.assertIsNone(init_config("foo"))
        test_value = "test"
        target_fields = [
            DBC.DB_HOST, DBC.DB_NAME, DBC.DB_PORT, DS.HOST, DS.ACCESS_TOKENS_URL,
            DS.CLICKSTREAMS_URL, DS.MONGODB_URL, DS.SQLDB_URL, FC.Data_dir, FC.MongoDB_FILE,
            FC.SQLDB_FILE, FC.META_DB_RECORD, FC.ACTIVE_VERSIONS, FC.STRUCTURES, TPK.Youtube_key,
            TPK.HKMooc_key, TPK.HKMooc_access_token
        ]
        expect_results = (len(target_fields)-1) * [test_value]
        expect_results.append(None)
        self.assertEqual(target_fields, expect_results)
