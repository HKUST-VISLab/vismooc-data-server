'''Http helper
'''
import urllib.request
import os
from os import path as path
import multiprocessing
import json
import time
# import hashlib
import ssl
import asyncio
import aiohttp


def head(url, headers=None, params=None, retry_times=5, delay=1):
    """Send synchronous head request

    """
    headers = headers or {}
    if params is not None:
        if isinstance(params, dict):
            url = url + '?'
            for key in params:
                url = url + key + '=' + params[key] + '&'
            url = url[0: -1]
        else:
            raise Exception("The params should be dict type")

    context = ssl.create_default_context()
    url = urllib.request.quote(url.encode('utf8'), ':/%?=&')
    req = urllib.request.Request(url=url, headers=headers, method='HEAD')
    for attempt_number in range(retry_times):
        try:
            print("Try "+str(attempt_number)+"th times to HEAD "+url+".")
            response = urllib.request.urlopen(req, context=context, timeout=100)
        except urllib.error.HTTPError as ex:
            print("HTTP HEAD error "+ ex.info()+" at "+url)
            time.sleep(delay)
        else:
            data = response.read()
            response_headers = response.info()
            return_code = response.getcode()
            return HttpResponse(return_code, response_headers, data)

def get(url, headers=None, params=None, retry_time=5, delay=1):
    """Send synchronous get request

    """
    headers = headers or {}
    if params is not None:
        if isinstance(params, dict):
            url = url + '?'
            for key in params:
                url = url + key + '=' + params[key] + '&'
            url = url[0: -1]
        else:
            raise Exception("The params should be dict type")

    context = ssl.create_default_context()
    url = urllib.request.quote(url.encode('utf8'), ':/%?=&')
    req = urllib.request.Request(url=url, headers=headers, method='GET')
    for attempt_number in range(retry_time):
        try:
            print("Try "+str(attempt_number)+"th times to GET "+url+".")
            response = urllib.request.urlopen(req, context=context)
        except urllib.error.HTTPError as ex:
            print("HTTP GET error "+ ex.info()+" at "+url)
            time.sleep(delay)
        else:
            data = response.read()
            response_headers = response.info()
            return_code = response.getcode()
            return HttpResponse(return_code, response_headers, data)


def post(url, headers=None, params=None, retry_time=5, delay=1):
    """Send synchronous post request

    """
    headers = headers or {}
    if params is not None:
        if isinstance(params, dict):
            raise Exception("The params should be dict type")

    req = urllib.request.Request(url=url, headers=headers, data=params, method='POST')
    for attempt_number in range(retry_time):
        try:
            print("Try "+str(attempt_number)+"th times to POST "+url+".")
            response = urllib.request.urlopen(req)
        except urllib.error.HTTPError as ex:
            print("HTTP POST error "+ex.info()+" at "+url)
            time.sleep(delay)
        else:
            data = response.read()
            response_headers = response.info()
            return_code = response.getcode()
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
        if isinstance(params, dict):
            raise Exception("The params should be dict type")

    async with session.get(url, headers=headers, params=params) as response:
        assert response.status >= 200 and response.status < 300
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
        if isinstance(params, dict):
            raise Exception("The params should be dict type")

    async with session.post(url, headers=headers, body=params) as response:
        assert response.status == 200
        data = await response.read()
        return HttpResponse(response.status, response.headers, data)


def get_list(urls, limit=30, headers=None, params=None):
    '''Get a list of urls async
    '''
    loop = asyncio.get_event_loop()
    results = []
    with aiohttp.ClientSession(loop=loop) as session:
        for i in range(0, len(urls), limit):
            tasks = [asyncio.ensure_future(async_get(url, headers, params, session))
                     for url in urls[i:i + limit]]
            results += loop.run_until_complete(asyncio.gather(*tasks))
    return [result.get_content() for result in results]


def download_single_file(url, file_path, headers, params=None, retry_time=5, delay=1):
    """Download file using one thread
    """
    print("opening url:", url)
    headers = headers or {}
    if params is not None:
        if isinstance(params, dict):
            url = url + '?'
            for key in params:
                url = url + key + '=' + params[key] + '&'
            url = url[0: -1]
        else:
            raise Exception("The params should be dict type")

    context = ssl.create_default_context()
    url = urllib.request.quote(url.encode('utf8'), ':/%?=&')
    req = urllib.request.Request(url=url, headers=headers, method='GET')
    for attempt_number in range(retry_time):
        try:
            print("Try "+str(attempt_number)+"th times to download "+url+".")
            response = urllib.request.urlopen(req, context=context)
        except urllib.error.HTTPError as ex:
            print("HTTP GET error "+ ex.info()+" at "+url)
            time.sleep(delay)
        else:
            return_code = response.getcode()
            response_headers = response.info()
            print(response_headers)
            file_total_length = response_headers[46]
            data_blocks = []
            total = 0
            progress_length = 100
            while True:
                block = response.read(1024)
                if not len(block):
                    break
                data_blocks.append(block)
                total += len(block)
                hash = ((progress_length*total)//fileTotalbytes)
                print("[{}{}] {}%".format('#' * hash, ' ' * (progress_length-hash), int(total/fileTotalbytes*100)), end="\r")
            data = b''.join(data_blocks)
            response.close()
            with open(file_path, 'wb+') as file:
                file.write(data)
            return file_path

def download_multi_files(urls, save_dir, common_suffix='', headers=None, \
    process_pool_size=(os.cpu_count() or 1)):
    """ Use multiprocess to download multiple files one time
    """
    headers = headers or {}
    if not isinstance(urls, list):
        raise Exception("The urls should be list type")
    if not path.exists(save_dir):
        raise Exception("The directory not exists")
    if len(urls) < 1:
        return []
    process_results = []
    pool = multiprocessing.Pool(processes=process_pool_size)
    for url in urls:
        file_path = path.join(path.abspath(save_dir), url[url.rindex("/") + 1:]) + common_suffix
        process_result = pool.apply_async(
            download_single_file, (url, file_path, headers))
        process_results.append(process_result)
    pool.close()
    pool.join()
    results = []
    for process_result in process_results:
        if process_result.get():
            results.append(process_result.get())
    return results

class HttpConnection:
    """This class is proposed to provide data-fetch interface

    """

    def __init__(self, host, headers=None):
        self.__host = host
        self.__headers = headers or {}

    @property
    def headers(self):
        '''return the headers
        '''
        return self.__headers

    @headers.setter
    def headers(self, headers):
        if not isinstance(headers, dict):
            raise TypeError('The headers require a dict variable !')
        self.__headers = headers

    def set_header(self, key, value):
        '''Set a field of header
        '''
        if key is None or value is None:
            raise TypeError('The key and value of a header must not be None')
        self.__headers[key] = value

    def get(self, url, params=None):
        '''The http GET method
        '''
        response = get(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.__headers["Cookie"] = response.get_headers().get("Set-Cookie")
        return response

    def head(self, url, params=None):
        '''The http HEAD method
        '''
        response = head(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.__headers["Cookie"] = response.get_headers().get("Set-Cookie")
        return response

    def post(self, url, params):
        '''The http POST method
        '''
        response = post(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.__headers["Cookie"] = response.get_headers().get("Set-Cookie")
        return response

    def download_files(self, urls, save_dir, common_suffix=''):
        '''Download a set of files
        '''
        return download_multi_files([self.__host + url for url in urls], save_dir,
                                    common_suffix=common_suffix, headers=self.__headers)

    async def async_get(self, url, params):
        '''The async http GET method
        '''
        response = await async_get(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.headers = {"Cookie": response.get_headers().get("Set-Cookie")}
        return response

    async def async_post(self, url, params):
        '''The async http POST method
        '''
        response = await async_post(self.__host + url, self.headers, params)
        if response.get_headers().get("Set-Cookie") is not None:
            self.headers = {"Cookie": response.get_headers().get("Set-Cookie")}
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
        return self.__headers or {}

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
