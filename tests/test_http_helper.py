'''Unit test of http_helper module
'''
# pylint: disable=C0111, C0103
import unittest
from unittest.mock import patch, MagicMock
import urllib.request
import mathematician.http_helper as http

class TestHTTPHelperClass(unittest.TestCase):
    '''Unit test of http_helper module
    '''

    def test_methods_with_right_params(self):
        pass

    def test_methods_with_wrong_params(self):
        url = "http://foo"
        with self.assertRaises(Exception, msg="The params of HEAD should be dict type"):
            http.head(url=url, params="adsf")
        with self.assertRaises(Exception, msg="The params of GET should be dict type"):
            http.get(url=url, params="asdf")
        with self.assertRaises(Exception, msg="The params of POST should be dict type"):
            http.post(url=url, params="asdf")
        with self.assertRaises(Exception, msg="The params of get_list should be dict type"):
            http.get_list(urls=url, params="asdf")
        with self.assertRaises(Exception, msg="The params of download_single_file should\
                               be dict type"):
            http.download_single_file(url, params="asdf")

    async def test_async_method_with_wrong_params(self):
        url = "http://foo"
        with self.assertRaises(Exception, msg="The params of async_GET should be dict type"):
            await http.async_get(url=url, params="asdf")
        with self.assertRaises(Exception, msg="The params of async_POST should be dict type"):
            await http.async_post(url=url, params="asdf")

    @patch('urllib.request.urlopen')
    def test_head_with_right_params(self, mock_urlopen):
        context_manager = MagicMock()
        context_manager.getcode.return_value = 200
        context_manager.read.return_value = "It is a return"
        mock_urlopen.return_value = context_manager

        response = http.head("http://foo.com")
        self.assertEqual(response.get_return_code(), 200, "The return code should be 200")
        self.assertEqual(response.get_content(), "It is a return",
                         "The return content should be `It is a return`")
    