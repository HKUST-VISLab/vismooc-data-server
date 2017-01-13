'''Test mongodb_helper module
'''
import unittest
from unittest.mock import patch, MagicMock
from mathematician.DB.mongo_dbhelper import MongoDB

class TestMongoDB(unittest.TestCase):
    '''Test the MongoDb helper
    '''

    @patch("mathematician.DB.mongo_dbhelper.MongoClient")
    def test_constructor(self, mock_mongo_client):
        '''test the constructor of MongoDB
        '''
        host = "localhost"
        db_name = "db"
        port = 27016
        mock_mongo_client = MagicMock()
        mock_mongo_client.return_value = {db_name:"asdf"}

        mongodb = MongoDB(host, db_name, port)
        self.assertIsInstance(mongodb, MongoDB, "New a mongoDB instance")
