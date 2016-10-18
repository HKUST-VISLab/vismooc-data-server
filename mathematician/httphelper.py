import urllib.request
import asyncio
import aiohttp
import os
import multiprocessing
import json


def get(url, headers={}, params=None):
    """Send synchronous get request

    """
    if params is not None:
        if type(params) is dict:
            url = url + '?'
            for key in params:
                url = url + key + '=' + params[key] + '&'
            url = url[0: -1]
        else:
            raise Exception("The params should be dict type")

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


async def async_get_list(urls, headers=None, params=None, loop=None):
    if type(urls) is not list:
        raise Exception("The urls should be list type")

    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = []
        for url in urls:
            task = asyncio.ensure_future(
                async_get(url, headers, params, session))

            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        return responses


def get_list(urls, limit=30, headers=None, params=None):
    loop = asyncio.get_event_loop()
    results = []
    for i in range(0, len(urls), limit):
        future = asyncio.ensure_future(async_get_list(urls[i:i + limit], headers, params, loop))

        loop.run_until_complete(future)
        results += future.result()
    return [result.get_content() for result in results]

async def async_download_file_part(url, start, end, file_path, params, headers={}, loop=None):
    """Download the given part, which is defined by start and end
    
    """
    headers = {"Range": "bytes={}-{}".format(start, end)}

    result = await async_get(url, headers=headers, params=params)
    result = result.get_content()
    with open(file_path, 'rb+') as f:
        f.seek(start, 0)
        f.write(result)


def download_single_file(url, file_path, file_slice=10 * 1024 * 1024, start=0, end=None, headers={}, params=None):

    req = urllib.request.Request(url, data=params, headers=headers, method='HEAD')

    with urllib.request.urlopen(req) as f:
        length = f.info()["Content-Length"]
    file_size = int(length)
    # if just download part of file
    end = end or (file_size - 1)
    download_size = end - start + 1
    if not os.path.exists(file_path):
        with open(file_path, 'w') as f:
            f.write('\0' * download_size)
            f.flush()
    tasks = []
    for part_start in range(0, download_size, file_slice):
        part_end = part_start + file_slice if part_start + \
            file_slice < download_size else download_size - 1

        future = asyncio.ensure_future(async_download_file_part(
            url, part_start, part_end, file_path, headers, params))

        tasks.append(future)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))


def download_multi_file(urls, save_dir, process_pool_size=(os.cpu_count() or 1)):
    if type(urls) is not list:
        raise Exception("The urls should be list type")
    if not os.path.exists(save_dir):
        raise Exception("The directory not exists")
    pool = multiprocessing.Pool(processes=process_pool_size)

    for url in urls:
        file_path = os.path.join(os.path.abspath(
            save_dir), url[url.rindex("/") + 1:])

        pool.apply_async(download_single_file, (url, file_path, ))
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
        if type(headers) is not dict:
            raise TypeError('The headers require a dict variable !')
        self.__headers = headers

    def set_header(self, key, value):
        if key is None or value is None:
            raise TypeError('The key and value of a header must not be None')
        self.__headers[key] = value

    def get(self, url, params=None):
        response = get(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.headers({"Cookie": response.get_headers().get("Set-Cookie")})

        return response

    def post(self, url, params):
        response = post(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.headers({"Cookie": response.get_headers().get("Set-Cookie")})

        return response

    async def async_get(self, url, params):
        response = await async_get(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.headers({"Cookie": response.get_headers().get("Set-Cookie")})

        return response

    async def async_post(self, url, params):
        response = await async_post(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.headers({"Cookie": response.get_headers().get("Set-Cookie")})

        return response



class DownloadFileFromServer():

    def __init__(self, api_key):
        self.__api_key = api_key
        self.__token = None
        self.__http_connection = HttpConnection("https://dataapi.hkmooc.hk/")

    def get_token_from_server(self):
        response = self.__http_connection.post(
            "/resources/access_token", {"API_Key": self.__api_key})

        response_json = json.loads(response)
        self.__token = response_json.get("collection").get("items")[
            0].get("access_token")



class HttpResponse():

    def __init__(self, return_code, headers, content):
        self.__return_code = return_code
        self.__headers = headers
        self.__content = content

    def get_headers(self):
        return self.__headers

    def get_content(self):
        return self.__content

    def get_return_code(self):
        return self.__return_code
