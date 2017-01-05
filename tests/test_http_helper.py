'''Unit test of http_helper module
'''
# pylint: disable=C0111, C0103
import unittest
from unittest.mock import patch, MagicMock
import mathematician.http_helper as http

class TestHTTPHelperClass(unittest.TestCase):
    '''Unit test of http_helper module
    '''

    @patch('urllib.request.urlopen')
    def test_methods_with_right_params(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = "It is a return"
        mock_response.info.return_value = {"a_header":"a_header"}
        mock_urlopen.return_value = mock_response

        url = "http://foo"
        params = {'a':1, 'b':2}
        http.head(url=url, params=params)
        args, kwargs = mock_urlopen.call_args
        full_url = args[0].get_full_url()
        right_url = url + '?'
        for key in params:
            right_url = right_url + key + '=' + str(params[key]) + '&'
        right_url = right_url[0: -1]
        self.assertEqual(full_url, right_url, "If pass params into the HEAD mehtod, the url should\
                         be a query string")

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
        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = "It is a return"
        mock_response.info.return_value = {"a_header":"a_header"}
        mock_urlopen.return_value = mock_response

        response = http.head("http://foo.com")
        self.assertEqual(response.get_return_code(), 200, "The return code should be 200")
        self.assertEqual(response.get_headers(), {"a_header":"a_header"}, "The return headers\
                         should be `{'a_header':'a_header'}`")
        self.assertEqual(response.get_content(), None, "The return content should be None")
