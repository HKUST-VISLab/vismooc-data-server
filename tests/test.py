import numpy
import sys
import asyncio
import aiohttp
sys.path.append('..')
from httphelper import HttpHelper



http_test = HttpHelper("http://www.google.com")
loop = asyncio.get_event_loop()

with aiohttp.ClientSession(loop=loop) as session:
    content = loop.run_until_complete(
        http_test.async_get(session, "/"))
    print(content)

loop.close()

#hello
print(numpy.abs(-5))
print('hello world')

#print(http_test.get("/"))
