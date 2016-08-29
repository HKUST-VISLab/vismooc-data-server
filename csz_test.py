import asyncio
import aiohttp
from httphelper import HttpHelper


http_test = HttpHelper("http://www.google.com")
loop = asyncio.get_event_loop()

with aiohttp.ClientSession(loop=loop) as session:
    content = loop.run_until_complete(
        http_test.async_get(session, "/"))
    print(content)

loop.close()

print("Hello")