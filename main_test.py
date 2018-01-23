import unittest
from test.HTMLrunner import HTMLTestRunner

if __name__ == '__main__':
    with open('testReport.html', 'wb') as file:
        suit = unittest.TestLoader().discover(start_dir='test')
        runner = HTMLTestRunner(stream=file, title='Report_title', description='Report_description', verbosity=2)
        runner.run(suit)
