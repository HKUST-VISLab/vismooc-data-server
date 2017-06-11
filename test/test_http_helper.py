'''Unit test of http_helper module
'''
# pylint: disable=C0111, C0103
import unittest
from logging import INFO
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError
from urllib.parse import urlencode

import asyncio
import mathematician.http_helper as http


def get_right_url(mock_object, url, params):
    args, kwargs = mock_object.call_args
    true_url = args[0].get_full_url()
    right_url = url + '?' + urlencode(params)
    return true_url, right_url


def async_test(func):
    def run_test(*args, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        loop.run_until_complete(func(*args, **kwargs))
        loop.close()
    return run_test


class AsyncContextManagerMock(MagicMock):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        for key in ('aenter_return', 'aexit_return'):
            setattr(self, key, kwargs[key] if key in kwargs else MagicMock())

    async def __aenter__(self):
        return self.aenter_return

    async def __aexit__(self, *args):
        return self.aexit_return


def AsyncFuncMock():
    coro = MagicMock(name="result")
    corofunc = MagicMock(name="CoroutineFunction",
                         side_effect=asyncio.coroutine(coro))
    corofunc.coro = coro
    return corofunc


class TestHTTPHelperMethods(unittest.TestCase):
    '''Unit test of http_helper module
    '''

    @patch('urllib.request.urlopen')
    def test_head(self, mock_urlopen):
        url = "http://foo.com"
        with self.assertRaises(Exception, msg="The params of HEAD should be dict type"):
            http.head(url=url, params="adsf")

        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = "It is a return"
        mock_response.info.return_value = {"a_header": "a_header"}
        mock_urlopen.return_value = mock_response
        params = {'a': 1, 'b': 2}

        http.head(url=url, params=params)
        true_url, right_url = get_right_url(mock_urlopen, url, params)
        self.assertEqual(true_url, right_url, "If pass params into the HEAD mehtod, the url should\
                         be a query string")

        response = http.head(url)
        self.assertEqual(response.get_return_code(), 200,
                         "The return code should be 200")
        self.assertEqual(response.get_headers(), {"a_header": "a_header"}, "The return headers\
                         should be `{'a_header':'a_header'}`")
        self.assertEqual(response.get_content(), None,
                         "The return content should be None")
        with self.assertLogs("vismooc", level=INFO) as cm:
            http.head(url)
        self.assertEqual(
            cm.output, ["INFO:vismooc:Try 0th times to HEAD " + url + "."])

        mock_urlopen.side_effect = HTTPError(
            url, 404, 'Not Found', {'a': 1}, None)
        with self.assertLogs("vismooc", level=INFO) as cm:
            http.head(url, retry_times=2, delay=0)
        self.assertEqual(cm.output, ["INFO:vismooc:Try 0th times to HEAD " + url + ".",
                                     "WARNING:vismooc:HTTP HEAD error 404 at " + url,
                                     "INFO:vismooc:Try 1th times to HEAD " + url + ".",
                                     "WARNING:vismooc:HTTP HEAD error 404 at " + url, ])

    @patch('urllib.request.urlopen')
    def test_get(self, mock_urlopen):
        url = "http://foo"
        with self.assertRaises(Exception, msg="The params of GET should be dict type"):
            http.get(url=url, params="asdf")

        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = "It is a return"
        mock_response.info.return_value = {"a_header": "a_header"}
        mock_urlopen.return_value = mock_response

        params = {'a': 11, 'b': 2}
        http.get(url=url, params=params)
        true_url, right_url = get_right_url(mock_urlopen, url, params)
        self.assertEqual(true_url, right_url, "If pass params into the GET mehtod, the url should\
                         be a query string")

        response = http.get(url)
        self.assertEqual(response.get_return_code(), 200,
                         "The return code should be 200")
        self.assertEqual(response.get_headers(), {"a_header": "a_header"}, "The return headers\
                         should be `{'a_header':'a_header'}`")
        self.assertEqual(response.get_content(), "It is a return",
                         "The return content should be a string")
        with self.assertLogs("vismooc", level=INFO) as cm:
            http.get(url)
        self.assertEqual(
            cm.output, ["INFO:vismooc:Try 0th times to GET " + url + "."])

        mock_urlopen.side_effect = HTTPError(
            url, 404, 'Not Found', {'a': 1}, None)
        with self.assertLogs("vismooc", level=INFO) as cm:
            http.get(url, retry_times=2, delay=0)
        self.assertEqual(cm.output, ["INFO:vismooc:Try 0th times to GET " + url + ".",
                                     "WARNING:vismooc:HTTP GET error 404 at " + url,
                                     "INFO:vismooc:Try 1th times to GET " + url + ".",
                                     "WARNING:vismooc:HTTP GET error 404 at " + url, ])

    @patch('urllib.request.urlopen')
    def test_post(self, mock_urlopen):
        url = "http://foo.com"
        with self.assertRaises(Exception, msg="The params of POST should be dict type"):
            http.post(url=url, params="asdf")

        mock_response = MagicMock()
        mock_response.getcode.return_value = 200
        mock_response.read.return_value = "It is a return"
        mock_response.info.return_value = {"a_header": "a_header"}
        mock_urlopen.return_value = mock_response

        params = {'a': 11, 'b': 2}
        with self.assertLogs("vismooc", level=INFO) as cm:
            http.post(url, params=params)
        self.assertEqual(
            cm.output, ["INFO:vismooc:Try 0th times to POST " + url + "."])

        mock_urlopen.side_effect = HTTPError(
            url, 404, 'Not Found', {'a': 1}, None)
        with self.assertLogs("vismooc", level=INFO) as cm:
            http.post(url, retry_times=2, delay=0)
        self.assertEqual(cm.output, ["INFO:vismooc:Try 0th times to POST " + url + ".",
                                     "WARNING:vismooc:HTTP POST error 404 at " + url,
                                     "INFO:vismooc:Try 1th times to POST " + url + ".",
                                     "WARNING:vismooc:HTTP POST error 404 at " + url, ])

    @patch('mathematician.http_helper.aiohttp.get')
    @async_test
    async def test_async_get(self, mock_aiohttp_get):
        url = "http://foo"
        with self.assertRaises(Exception, msg="The params of async_GET should be dict type"):
            await http.async_get(url=url, params="asdf")

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"a_header": "a_header"}
        async def coro_read():
            return "It is a return"
        mock_response.read = coro_read
        mock_aiohttp_get.return_value = AsyncContextManagerMock(
            name="mock_response", aenter_return=mock_response)
        params = {'a': 1, 'b': 2}
        response = await http.async_get(url=url, params=params)
        self.assertEqual(response.get_return_code(), 200,
                         "The return code should be 200")
        self.assertEqual(response.get_headers(), {"a_header": "a_header"}, "The return headers\
                         should be `{'a_header':'a_header'}`")
        self.assertEqual(response.get_content(), "It is a return",
                         "The return content should be a string")

    def test_get_list(self):
        url = "http://foo"
        with self.assertRaises(Exception, msg="The params of get_list should be dict type"):
            http.get_list(urls=url, params="asdf")
