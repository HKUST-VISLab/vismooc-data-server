import http.client
import urllib.request
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

    def get(self, url, params=None):
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

        req = urllib.request.Request(
            url=self.host + url, headers=self.http_header, method='GET')

        with urllib.request.urlopen(req) as f:
            data = f.read()
            return data

    def post(self, url, params=None):
        """Send synchronous post request
    
        """
        if params is not None:
            if type(params) is dict:
                post_params = params
            else:
                raise Exception("The params should be dict type")

        req = urllib.request.Request(
            url=self.host + url, data=post_params, headers=self.http_header, method='POST')

        with urllib.request.urlopen(req) as f:
            data = f.read()
            return data

    async def async_get(self, url, params=None):
        """Send asynchronous get request
            Example for use:
                http_test = HttpHelper("http://www.google.com")
                loop = asyncio.get_event_loop()
                content = loop.run_until_complete(
                    http_test.async_get("/"))
                print(content)
                loop.close()
        """
        if params is not None:
            if type(params) is dict:
                url = url + '?'
                for key in params:
                    url = url + key + '=' + params[key] + '&'
                url = url[0: -1]
            else:
                raise Exception("The params should be dict type")

        url = self.host + url
        async with aiohttp.ClientSession() as session:
            with aiohttp.Timeout(10):
                async with session.get(url, headers=self.http_header) as response:
                    assert response.status == 200
                    result = await response.read()
                    return result

    async def async_post(self, session, url, params=None):
        """Send asynchronous post request

            Example for use:
                http_test = HttpHelper("http://www.google.com")
                loop = asyncio.get_event_loop()

                with aiohttp.ClientSession(loop=loop) as session:
                    content = loop.run_until_complete(
                        http_test.async_get(session, "/"))
                    print(content)
            loop.close()
        """
        if params is not None:
            if type(params) is dict:
                post_params = params
            else:
                raise Exception("The params should be dict type")
        url = self.host + url
        with aiohttp.Timeout(10):
            async with session.request("POST", url, body=post_params, headers=self.http_header) as response:
                assert response.status == 200
                result = await response.read()
                return result
