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

    @patch('mathematician.http_helper.aiohttp.ClientSession')
    @async_test
    async def test_async_get(self, mock_aiohttp_ClientSession):
        url = "http://foo"
        with self.assertRaises(Exception, msg="The params of async_GET should be dict type"):
            await http.async_get(url=url, params="asdf")

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"a_header": "a_header"}
        async def coro_read():
            return "It is a return"
        mock_response.read = coro_read
        mock_aiohttp_get = MagicMock()
        mock_aiohttp_get.return_value = AsyncContextManagerMock(
            name="mock_response", aenter_return=mock_response)
        mock_session = MagicMock()
        mock_session.get = mock_aiohttp_get
        mock_aiohttp_ClientSession.return_value = mock_session
        params = {'a': 1, 'b': 2}
        response = await http.async_get(url=url, params=params)
        self.assertEqual(response.get_return_code(), 200,
                         "The return code should be 200")
        self.assertEqual(response.get_headers(), {"a_header": "a_header"}, "The return headers\
                         should be `{'a_header':'a_header'}`")
        self.assertEqual(response.get_content(), "It is a return",
                         "The return content should be a string")

    @patch('mathematician.http_helper.aiohttp.ClientSession')
    @async_test
    async def test_async_post(self, mock_aiohttp_ClientSession):
        url = "http://boo"
        with self.assertRaises(Exception, msg="The param of async_post should be dict type"):
            await http.async_post(url=url, params="asdf")

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"a_header": "a_header"}
        async def coro_read():
            return "It is a return"
        mock_response.read = coro_read
        mock_aiohttp_post = MagicMock()
        mock_aiohttp_post.return_value = AsyncContextManagerMock(
            name="mock_response", aenter_return=mock_response)
        mock_session = MagicMock()
        mock_session.post = mock_aiohttp_post
        mock_aiohttp_ClientSession.return_value = mock_session
        params = {'a': 1, 'b': 2}
        response = await http.async_post(url=url, params=params)
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

    def test_download_single_file(self):
        url = "http://foo"
        with self.assertRaises(Exception, msg="The params of download_single_file should be dict type"):
            http.download_single_file(url, params="asdf")


class TestHTTPHelperClass(unittest.TestCase):

    def test_constructor(self):
        host = 'local'
        header = {'a': 'asdf', 'b': 'asdfqwe'}
        conn = http.HttpConnection(host, header)
        self.assertDictEqual(header, conn.headers,
                             'the header should be set in constructor')

    def test_headers_setter(self):
        host = 'local'
        header = {'a': 'asdf', 'b': 'asdfqwe'}
        new_header = {'asd': 'a'}
        conn = http.HttpConnection(host, header)
        self.assertDictEqual(header, conn.headers,
                             'the header should be set in constructor')
        with self.assertRaises(TypeError, msg="The headers require a dict variable !"):
            conn.headers = 1
        conn.headers = new_header
        self.assertDictEqual(conn.headers, new_header, 'reset the headers')

    def test_set_header(self):
        host = 'local'
        header = {'a': 'asdf', 'b': 'asdfqwe'}
        new_header = {'a': 'asdf', 'b': 111}
        conn = http.HttpConnection(host, header)
        self.assertDictEqual(header, conn.headers,
                             'the header should be set in constructor')
        with self.assertRaises(TypeError, msg='The key of a header should not be None'):
            conn.set_header(None, 111)
        with self.assertRaises(TypeError, msg='The value of a header should not be None'):
            conn.set_header('b', None)
        conn.set_header('b', 111)
        self.assertDictEqual(conn.headers, new_header,
                             'should update a header in the headers')

    @patch('mathematician.http_helper.get')
    def test_get(self, mock_get):
        host = "http://foo"
        headers = {"a_header": "a_header"}
        conn = http.HttpConnection(host, headers)
        mock_get.return_value = None

        with self.assertLogs("vismooc", level=INFO) as cm:
            self.assertIsNone(conn.get('/'), msg="return a None")
        self.assertEqual(
            cm.output, ["WARNING:vismooc:The response of HttpConnection GET is None"])

        response_headers = {'Set-Cookie': 'It is a cookie'}
        mock_response = MagicMock()
        mock_response.get_headers.return_value = response_headers
        mock_get.return_value = mock_response

        self.assertIs(conn.get('/'), mock_response, 'Get the response')
        args = mock_get.call_args[0]
        self.assertTupleEqual(args, (host + '/', conn.headers, None, 5, 1), msg="The default args for\
            get method")
        self.assertEqual(conn.headers.get('Cookie'), response_headers.get('Set-Cookie'), 'set the cookies if\
            has "Set-Cookie" in the response headers')

        params = {'a': 1, 'b': 2}
        retry_times = 4
        delay = 100
        self.assertIs(conn.get('/', params, retry_times, delay),
                      mock_response, 'Get the response')
        args = mock_get.call_args[0]
        self.assertTupleEqual(args, (host + '/', conn.headers, params, retry_times, delay), msg="The\
            args should be passed to get method")

    @patch('mathematician.http_helper.head')
    def test_head(self, mock_head):
        host = "http://foo"
        headers = {"a_header": "a_header"}
        conn = http.HttpConnection(host, headers)
        mock_head.return_value = None

        with self.assertLogs("vismooc", level=INFO) as cm:
            self.assertIsNone(conn.head('/'), msg="return a None")
        self.assertEqual(
            cm.output, ["WARNING:vismooc:The response of HttpConnection HEAD is None"])

        response_headers = {'Set-Cookie': 'It is a cookie'}
        mock_response = MagicMock()
        mock_response.get_headers.return_value = response_headers
        mock_head.return_value = mock_response

        self.assertIs(conn.head('/'), mock_response, 'Head the response')
        args = mock_head.call_args[0]
        self.assertTupleEqual(args, (host + '/', conn.headers, None, 5, 1), msg="The default args for\
            head method")
        self.assertEqual(conn.headers.get('Cookie'), response_headers.get('Set-Cookie'), 'set the cookies if\
            has "Set-Cookie" in the response headers')

        params = {'a': 1, 'b': 2}
        retry_times = 4
        delay = 100
        self.assertIs(conn.head('/', params, retry_times, delay),
                      mock_response, 'Head the response')
        args = mock_head.call_args[0]
        self.assertTupleEqual(args, (host + '/', conn.headers, params, retry_times, delay), msg="The\
            args should be passed to head method")

    @patch('mathematician.http_helper.post')
    def test_post(self, mock_post):
        host = "http://foo"
        headers = {"a_header": "a_header"}
        conn = http.HttpConnection(host, headers)
        mock_post.return_value = None

        params = {'a': 1, 'b': 2}
        with self.assertLogs("vismooc", level=INFO) as cm:
            self.assertIsNone(conn.post('/', params), msg="return a None")
        self.assertEqual(
            cm.output, ["WARNING:vismooc:The response of HttpConnection POST is None"])

        response_headers = {'Set-Cookie': 'It is a cookie'}
        mock_response = MagicMock()
        mock_response.get_headers.return_value = response_headers
        mock_post.return_value = mock_response

        self.assertIs(conn.post('/', params),
                      mock_response, 'Post the response')
        args = mock_post.call_args[0]
        self.assertTupleEqual(args, (host + '/', conn.headers, params, 5, 1), msg="The default args for\
            post method")
        self.assertEqual(conn.headers.get('Cookie'), response_headers.get('Set-Cookie'), 'set the cookies if\
            has "Set-Cookie" in the response headers')

        params = {'a': 1, 'b': 2}
        retry_times = 4
        delay = 100
        self.assertIs(conn.post('/', params, retry_times, delay),
                      mock_response, 'Post the response')
        args = mock_post.call_args[0]
        self.assertTupleEqual(args, (host + '/', conn.headers, params, retry_times, delay), msg="The\
            args should be passed to post method")

    @patch('mathematician.http_helper.async_get', new_callable=AsyncFuncMock)
    @async_test
    async def test_async_get(self, mock_async_get):
        host = "http://foo"
        headers = {"a_header": "a_header"}
        conn = http.HttpConnection(host, headers)
        mock_async_get.coro.return_value = None

        with self.assertLogs("vismooc", level=INFO) as cm:
            self.assertIsNone(await conn.async_get('/', None), msg="return a None")
        self.assertEqual(
            cm.output, ["WARNING:vismooc:The response of HttpConnection async_GET is None"])

        response_headers = {'Set-Cookie': 'It is a cookie'}
        mock_response = MagicMock()
        mock_response.get_headers.return_value = response_headers
        mock_response = MagicMock()
        mock_response.get_headers.return_value = response_headers
        mock_async_get.coro.return_value = mock_response
        self.assertIs(await conn.async_get('/', None), mock_response, 'Get the response')
        args = mock_async_get.call_args[0]
        self.assertTupleEqual(args, (host + '/', conn.headers, None), msg="The\
                default args for get method")
        self.assertEqual(conn.headers.get('Cookie'), response_headers.get('Set-Cookie'), 'set the cookies if\
            has "Set-Cookie" in the response headers')

        input_params = {'a': 1, 'b': 2}
        self.assertIs(await conn.async_get('/', input_params), mock_response, 'Get the response')
        args = mock_async_get.call_args[0]
        self.assertTupleEqual(args, (host + '/', conn.headers, input_params),
                              msg="The args should be passed to get method")

    @patch('mathematician.http_helper.async_post', new_callable=AsyncFuncMock)
    @async_test
    async def test_async_post(self, mock_async_post):
        host = "http://foo"
        headers = {"a_header": "a_header"}
        conn = http.HttpConnection(host, headers)
        mock_async_post.coro.return_value = None

        with self.assertLogs("vismooc", level=INFO) as cm:
            self.assertIsNone(await conn.async_post('/', None), msg="return a None")
        self.assertEqual(
            cm.output, ["WARNING:vismooc:The response of HttpConnection async_POST is None"])

        response_headers = {'Set-Cookie': 'It is a cookie'}
        mock_response = MagicMock()
        mock_response.get_headers.return_value = response_headers
        mock_response = MagicMock()
        mock_response.get_headers.return_value = response_headers
        mock_async_post.coro.return_value = mock_response
        self.assertIs(await conn.async_post('/', None), mock_response, 'Get the response')
        args = mock_async_post.call_args[0]
        self.assertTupleEqual(args, (host + '/', conn.headers, None), msg="The\
                default args for get method")
        self.assertEqual(conn.headers.get('Cookie'), response_headers.get('Set-Cookie'), 'set the cookies if\
            has "Set-Cookie" in the response headers')

        input_params = {'a': 1, 'b': 2}
        self.assertIs(await conn.async_post('/', input_params), mock_response, 'Get the response')
        args = mock_async_post.call_args[0]
        self.assertTupleEqual(args, (host + '/', conn.headers, input_params),
                              msg="The args should be passed to get method")
