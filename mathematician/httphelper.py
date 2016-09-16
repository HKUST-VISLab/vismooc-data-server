import urllib.request
import asyncio
import aiohttp


class HttpHelper:
    """This class is proposed to provide data-fetch interface

    """

    def __init__(self, host):
        self.host = host
        self.http_header = {}

    def header(self, *params):
        if len(params) == 0:
            return self.http_header
        elif len(params) == 1:
            if type(params[0]) is dict:
                self.http_header = params[0]
            else:
                raise TypeError('The header require a dict variable !')
        elif len(params) == 2:
            if type(params[0]) is str and type(params[1]) is str:
                self.http_header[params[0]] = params[1]
            else:
                raise TypeError(
                    'Each header items require two string to set !')
        else:
            raise Exception('too many arguments !')

    @classmethod
    def get(cls, url, params=None):
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

        req = urllib.request.Request(url=url, method='GET')

        with urllib.request.urlopen(req) as f:
            data = f.read()
            return data

    @classmethod
    def post(cls, url, params=None):
        """Send synchronous post request
    
        """
        if params is not None:
            if type(params) is not dict:
                raise Exception("The params should be dict type")

        req = urllib.request.Request(url=url, data=params, method='POST')

        with urllib.request.urlopen(req) as f:
            data = f.read()
            return data

    @classmethod
    async def async_get(cls, url, params=None, session=aiohttp):
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

        async with session.get(url, params=params) as response:
            assert response.status == 200
            result = await response.json()
            return result

    @classmethod
    async def async_post(clf, url, params=None, session=aiohttp):
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

        async with session.post(url, body=params) as response:
            assert response.status == 200
            result = await response.read()
            return result

    @classmethod
    async def async_get_list(cls, urls, loop=None, params=None):
        if type(urls) is not list:
            raise Exception("The urls should be list type")

        async with aiohttp.ClientSession(loop=loop) as session:
            tasks = []
            for url in urls:
                task = asyncio.ensure_future(
                    cls.async_get(url, session=session))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
            return responses

    @classmethod
    def get_list(cls, urls, limit=30, params=None):
        loop = asyncio.get_event_loop()
        results = []
        for i in range(0, len(urls), limit):
            future = asyncio.ensure_future(
                cls.async_get_list(urls[i:i + limit], params, loop))
            loop.run_until_complete(future)
            results += future.result()
        return results
