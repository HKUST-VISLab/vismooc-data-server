import urllib.request
import asyncio
import os
import multiprocessing
import json
import hashlib
import aiohttp

def get(url, headers={}, params=None):
    """Send synchronous get request

    """
    if params is not None:
        if isinstance(params, dict):
            url = url + '?'
            for key in params:
                url = url + key + '=' + params[key] + '&'
            url = url[0: -1]
        else:
            raise Exception("The params should be dict type")

    # print(url)
    req = urllib.request.Request(url=url, headers=headers, method='GET')
    with urllib.request.urlopen(req) as f:
        assert f.getcode() >= 200 and f.getcode() < 300
        data = f.read()
        response_headers = f.info()
        return_code = f.getcode()
        return HttpResponse(return_code, response_headers, data)


def post(url, headers={}, params=None):
    """Send synchronous post request

    """
    if params is not None:
        if type(params) is not dict:
            raise Exception("The params should be dict type")

    req = urllib.request.Request(
        url=url, headers=headers, data=params, method='POST')


    with urllib.request.urlopen(req) as f:
        assert f.getcode() >= 200 and f.getcode() < 300
        data = f.read()
        response_headers = f.info()
        return_code = f.getcode()
        return HttpResponse(return_code, response_headers, data)


async def async_get(url, headers=None, params=None, session=aiohttp):
    """Out of dated
        Send asynchronous get request
        Example for use:
            http_test = HttpHelper("http://www.google.com")
            loop = asyncio.get_event_loop()
            content = loop.run_until_complete(
                http_test.async_get("/"))
            print(content)
            loop.close()
    """
    if params is not None:
        if type(params) is not dict:
            raise Exception("The params should be dict type")

    async with session.get(url, headers=headers, params=params) as response:
        assert response.status >= 200 and response.status < 300
        # change "result = await response.json()" to "result = await
        # response.read()"
        data = await response.read()
        return HttpResponse(response.status, response.headers, data)

async def async_post(url, headers=None, params=None, session=aiohttp):
    """Out of dated
        Send asynchronous post request

        Example for use:
            http_test = HttpHelper("http://www.google.com")
            loop = asyncio.get_event_loop()
            content = loop.run_until_complete(
                http_test.async_get(session, "/"))
            print(content)
            loop.close()
    """
    if params is not None:
        if type(params) is not dict:
            raise Exception("The params should be dict type")

    async with session.post(url, headers=headers, body=params) as response:
        assert response.status == 200
        data = await response.read()
        return HttpResponse(response.status, response.headers, data)

def get_list(urls, limit=30, headers=None, params=None):
    loop = asyncio.get_event_loop()
    results = []
    with aiohttp.ClientSession(loop=loop) as session:
        for i in range(0, len(urls), limit):
            tasks = [ asyncio.ensure_future(async_get(url, headers, params, session)) for url in urls[i:i+limit]]
            results += loop.run_until_complete(asyncio.gather(*tasks))
    return [result.get_content() for result in results]


def download_single_file(url, file_path, headers, params=None):
    """Download file using one thread
    """
    result = get(url, headers=headers, params=params)
    result = result.get_content()
    with open(file_path, 'wb+') as file:
        file.write(result)

def download_multi_files(urls, save_dir, common_suffix='', headers={}, process_pool_size=(os.cpu_count() or 1)):
    """ Use multiprocess to download multiple files one time
    """
    if not isinstance(urls, list):
        raise Exception("The urls should be list type")
    if not os.path.exists(save_dir):
        raise Exception("The directory not exists")
    pool = multiprocessing.Pool(processes=process_pool_size)

    for url in urls:
        file_path = os.path.join(os.path.abspath(save_dir), url[url.rindex("/")+1 : ]) + common_suffix
        # pool.apply_async(download_single_file, (url, file_path, headers))
        pool.apply_async(download_single_file, (url, file_path, headers))

    pool.close()
    pool.join()


class HttpConnection:
    """This class is proposed to provide data-fetch interface

    """

    def __init__(self, host, headers=None):
        self.__host = host
        self.__headers = headers or {}

    @property
    def headers(self):
        return self.__headers

    @headers.setter
    def headers(self, headers):
        if not isinstance(headers, dict):
            raise TypeError('The headers require a dict variable !')
        self.__headers = headers

    def set_header(self, key, value):
        if key is None or value is None:
            raise TypeError('The key and value of a header must not be None')
        self.__headers[key] = value

    def get(self, url, params=None):
        response = get(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.headers = {"Cookie" : response.get_headers().get("Set-Cookie")}
        return response

    def post(self, url, params):
        response = post(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.headers = {"Cookie" : response.get_headers().get("Set-Cookie")}
        return response

    def download_files(self, urls, save_dir, common_suffix=''):
        return download_multi_files(urls, save_dir, common_suffix=common_suffix, headers=self.__headers)

    async def async_get(self, url, params):
        response = await async_get(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.headers = {"Cookie" : response.get_headers().get("Set-Cookie")}
        return response

    async def async_post(self, url, params):
        response = await async_post(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.headers = {"Cookie" : response.get_headers().get("Set-Cookie")}
        return response

class HttpResponse():
    """ Encapsulate http response headers, content, and status code in this class
    """
    def __init__(self, return_code, headers, content):
        self.__return_code = return_code
        self.__headers = headers
        self.__content = content

    def get_headers(self):
        """ return the response headers
        """
        return self.__headers

    def get_content(self):
        """ return the response content in bytes
        """
        return self.__content

    def get_return_code(self):
        """ return the response status code
        """
        return self.__return_code
    def get_content_json(self, encode="UTF-8"):
        """ return the response content in json
        """
        return json.loads(str(self.__content, encode))

