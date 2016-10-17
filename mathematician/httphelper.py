import urllib.request
import asyncio
import aiohttp
import os
import multiprocessing


def get(url, headers=None, params=None):
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
        data = f.read()
        return data


def post(url, headers=None, params=None):
    """Send synchronous post request

    """
    if params is not None:
        if type(params) is not dict:
            raise Exception("The params should be dict type")

    req = urllib.request.Request(url=url, headers=headers, data=params, method='POST')

    with urllib.request.urlopen(req) as f:
        data = f.read()
        return data


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
        #change "result = await response.json()" to "result = await response.read()"
        result = await response.read()
        return result

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
        result = await response.read()
        return result


async def async_get_list(urls, headers=None, params=None, loop=None):
    if type(urls) is not list:
        raise Exception("The urls should be list type")

    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = []
        for url in urls:
            task = asyncio.ensure_future(async_get(url, headers, params, session))
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
    return results

async def async_download_file_part(url, start, end, file_path, params, headers={}, loop=None):
    """Download the given part, which is defined by start and end"""
    #print("here")
    tmp = "bytes={}-{}".format(start, end)
    headers = {"Range" : "bytes={}-{}".format(start, end)}
    result = await async_get(url, headers=headers, params=params)
    #print(start)
    with open(file_path, 'rb+') as f:
        f.seek(start, 0)
        f.write(result)

def download_single_file(url, file_path, file_slice=10*1024*1024, start=0, end=None, headers={}, params=None):
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
        part_end = part_start+file_slice if part_start+file_slice < download_size else download_size-1
        future = asyncio.ensure_future(async_download_file_part(url, part_start, part_end, file_path, headers, params))
        tasks.append(future)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))

def download_multi_file(urls, save_dir, process_pool_size=(os.cpu_count() or 1)):
    if type(urls) is not list:
        raise Exception("The urls should be list type")
    if not os.path.exists(save_dir):
        raise Exception("The directory not exists")
    pool = multiprocessing.Pool(processes = process_pool_size)
    for url in urls:
        file_path = os.path.join(os.path.abspath(save_dir), url[url.rindex("/")+1 : ])
        pool.apply_async(download_single_file, (url, file_path, ))
    pool.close()
    pool.join()

class HttpConnection:
    """This class is proposed to provide data-fetch interface

    """

    def __init__(self, host, headers=None):
        self._session = aiohttp.ClientSession()
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
        return get(self.__host + url, self.headers, params)

    def post(self, url, params):
        return post(self.__host + url, self.headers, params)

    async def async_get(self, url, params):
        result = await async_get(self.__host + url, self.headers, params, session=self._session)
        return result

    async def async_post(self, url, params):
        result = await async_post(self.__host + url, self.headers, params, session=self._session)
        return result
        
