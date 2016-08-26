import numpy
import sys
sys.path.append('..')
from httphelper import HttpHelper

print(numpy.abs(-5))
print('hello world')

http_test = HttpHelper("www.google.com")
result = http_test.get("/")
print(result)


