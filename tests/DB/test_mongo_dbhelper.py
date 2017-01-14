'''Test mongodb_helper module
'''
import unittest
from unittest.mock import patch, MagicMock
from mathematician.DB.mongo_dbhelper import MongoDB

class TestMongoDB(unittest.TestCase):
    '''Test the MongoDb helper
    '''

    # @patch("mathematician.DB.mongo_dbhelper.MongoClient")
    def test_constructor(self):
        '''test the constructor of MongoDB
        '''
        host = "127.0.0.1"
        db_name = "db"
        port = 27017

        mongodb = MongoDB(host, db_name, port)
        self.assertIsInstance(mongodb, MongoDB, "New a mongoDB instance")
