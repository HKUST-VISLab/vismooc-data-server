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

    def tearDown(self):
        self.mongodb.clear()

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

    def test_get_collection_names(self):
        '''test get_collection_names of a database
        '''
        name1 = "test_collection_1"
        name2 = "test_collection_2"
        self.mongodb.create_collection(name1)
        self.mongodb.create_collection(name2)
        self.assertEqual([name1, name2], self.mongodb.get_collection_names(),
                         "get_collection_names will return all exist collections")

    def test_get_collection(self):
        '''test get_collection of a database
        '''
        name = "test_collection"
        collection = self.mongodb.get_collection(name)
        self.assertIsInstance(collection, MongoCollection,
                              "get_collection will return a MongoCollection instance")


    def test_add_user(self):
        '''test add_user of a database
        '''
        username = "test_user"
        passwd = "passwd"
        self.mongodb.add_user(username, passwd)
        users = self.mongodb.users_info()
        usernames = [user.get('user') for user in users]
        self.assertIn(username, usernames, "add_user should create a new user")

    def test_users_info(self):
        '''test users_info of a database
        '''
        username = "test_user"
        username1 = "test_user1"
        passwd = "passwd"
        self.mongodb.add_user(username, passwd)
        self.mongodb.add_user(username1, passwd)
        users = self.mongodb.users_info()
        usernames = [user.get('user') for user in users]
        self.assertEqual([username, username1], usernames, "users_info should list all users")
