'''The entrance of all test case
'''
import glob
import os
import sys
import unittest

def get_tests():
    '''Get all test code
    '''
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    test_files = glob.glob('tests/**/test_*.py')
    module_strings = [test_file[6:len(test_file) - 3].replace('/', '.') for test_file in test_files]
    suites = unittest.TestLoader().loadTestsFromNames(module_strings)
    return unittest.TestSuite(suites)
