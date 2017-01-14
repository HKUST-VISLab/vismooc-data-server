'''Test mongodb_helper module
'''
import unittest
# from unittest.mock import patch, MagicMock
from mathematician.DB.mongo_dbhelper import MongoDB, MongoCollection


class TestMongoDB(unittest.TestCase):
    '''Test the MongoDb helper
    '''

    def setUp(self):
        host = "127.0.0.1"
        db_name = "db"
        port = 27017
        self.mongodb = MongoDB(host, db_name, port)

    # @patch("mathematician.DB.mongo_dbhelper.MongoClient")
    def test_constructor(self):
        '''test the constructor of MongoDB
        '''
        self.assertIsInstance(self.mongodb, MongoDB, "New a mongoDB instance")

    def test_create_collection(self):
        '''test create a collection of a MongoDB
        '''
        name = "test_collection"
        collection = self.mongodb.create_collection(name)
        self.assertIsInstance(collection, MongoCollection, "Create a MongoCollection instance")
        self.assertIn(name, self.mongodb.get_collection_names(),
                      "New collection is created secessfully")
