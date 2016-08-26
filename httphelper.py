import http.client


class HttpHelper:

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
        if len(params) == 1 and isinstance(params[0], (dict)):
            post_params = params[0]
        elif len(params) > 1:
            raise Exception("too many arguments !")
        conn = http.client.HTTPConnection(self.host)
        conn.request("POST", url, post_params, self.header)
        response = conn.getresponse()
        data = response.read()
        conn.close()
        return data


