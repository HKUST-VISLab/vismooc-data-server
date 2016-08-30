import http.client
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
            if isinstance(params[0], (dict,)):
                self.http_header = params[0]
            else:
                raise TypeError('The header require a dict variable !')
        elif len(params) == 2:
            if isinstance(params[0], (str)) and isinstance(params[1], (str)):
                self.http_header[params[0]] = params[1]
            else:
                raise TypeError('Each header items require two string to set !')
        else:
            raise Exception('too many arguments !')


    
    def get(self, url, *params):
        """Send synchronous get request
    
        """
        if len(params) == 1 and isinstance(params[0], (dict)):
            url = url + '?'
            for key in params[0]:
                url = url + key + '=' + params[0][key] + '&'
            url = url[0: -1]
        elif len(params) > 1:
            raise Exception("too many arguments !")

        conn = http.client.HTTPConnection(self.host)
        conn.request("GET", url, self.http_header)
        response = conn.getresponse()
        data = response.read()
        conn.close()
        return data

    
    def post(self, url, *params):
        """Send synchronous post request
    
        """
        if len(params) == 1 and isinstance(params[0], (dict)):
            post_params = params[0]
        elif len(params) > 1:
            raise Exception("too many arguments !")
        conn = http.client.HTTPConnection(self.host)
        conn.request("POST", url, body=post_params, headers=self.header)
        response = conn.getresponse()
        data = response.read()
        conn.close()
        return data
    

    

    async def async_get(self, session, url, *params):
        """Send asynchronous get request
    
            Example for use:
                http_test = HttpHelper("http://www.google.com")
                loop = asyncio.get_event_loop()

                with aiohttp.ClientSession(loop=loop) as session:
                    content = loop.run_until_complete(
                        http_test.async_get(session, "/"))
                    print(content)

                loop.close()
        """
        if len(params) == 1 and isinstance(params[0], (dict)):
            url = url + '?'
            for key in params[0]:
                url = url + key + '=' + params[0][key] + '&'
            url = url[0: -1]
        elif len(params) > 1:
            raise Exception("too many arguments !")
        
        url = self.host + url
        with aiohttp.Timeout(10):
            async with session.request("GET", url, headers=self.http_header) as response:
                assert response.status == 200
                result = await response.read()
                return result
        

    
    async def async_post(self, session, url, *params):
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
        if len(params) == 1 and isinstance(params[0], (dict)):
            post_params = params[0]
        elif len(params) > 1:
            raise Exception("too many arguments !")
        
        url = self.host + url
        with aiohttp.Timeout(10):
            async with session.request("POST", url, body=post_params, headers=self.http_header) as response:
                assert response.status == 200
                result = await response.read()
                return result
    

