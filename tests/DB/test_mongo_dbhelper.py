'''Test mongodb_helper module
'''
import unittest
# from unittest.mock import patch, MagicMock
from mathematician.DB.mongo_dbhelper import MongoDB, MongoCollection, MongoClient


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

    def test_constructor(self):
        '''test the constructor of MongoDB
        '''
        test_host = 1234
        test_name = 1234
        with self.assertRaises(TypeError, msg="db_name should be instance of str"):
            MongoDB("host", test_name)
        with self.assertRaises(TypeError, msg="host should be instance of str"):
            MongoDB(test_host, "db")
        self.assertIsInstance(self.mongodb, MongoDB, "New a mongoDB instance")

    def test_create_collection(self):
        '''test create a collection of a MongoDB
        '''
        wrong_name = 1234
        with self.assertRaises(TypeError, msg="create_collection: collection name should be\
                               instance of str"):
            self.mongodb.create_collection(wrong_name)
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
        wrong_name = 1234
        with self.assertRaises(TypeError, msg="get_collection: collection name should be instance\
                               of str"):
            self.mongodb.get_collection(wrong_name)
        name = "test_collection"
        collection = self.mongodb.get_collection(name)
        self.assertIsInstance(collection, MongoCollection,
                              "get_collection will return a MongoCollection instance")


    def test_add_user(self):
        '''test add_user of a database
        '''
        wrong_username = 1234
        wrong_passwd = None
        with self.assertRaises(TypeError, msg="add_user: username should be instance of str"):
            self.mongodb.add_user(wrong_username, "asdf")
        with self.assertRaises(TypeError, msg="add_user: passwd should not be empty"):
            self.mongodb.add_user("asdf", wrong_passwd)
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

    def test_clear(self):
        '''Test the clear of a database
        '''
        db_name = "db"
        self.mongodb.get_collection('test').insert_one({'a':True})
        client = MongoClient("127.0.0.1", 27017)
        db_names = client.database_names()
        self.assertIn(db_name, db_names, "Before clear(), db `db` exists")
        self.mongodb.clear()
        db_names = client.database_names()
        self.assertNotIn(db_name, db_names, "After clear(), db `db` does not exists")

class TestMongoCollection(unittest.TestCase):
    '''Test the MongoCollection class
    '''
    def setUp(self):
        host = "127.0.0.1"
        port = 27017
        self.db_name = "db"
        self.collection_name = "test_collection"
        self.mongodb = MongoDB(host, self.db_name, port)
        self.collection = self.mongodb.create_collection(self.collection_name)

    def tearDown(self):
        self.mongodb.clear()

    def test_constructor(self):
        '''Test the constructor
        '''
        self.assertIsInstance(self.collection, MongoCollection, "New a MongoCollection instance")

    def test_insert_one(self):
        '''Test the insert_one()
        '''
        document_id = "asdfsdf"
        document = {'id':document_id}
        self.collection.insert_one(document)
        client = MongoClient("127.0.0.1", 27017)
        db_names = client.database_names()
        self.assertIn(self.db_name, db_names, "After insert_one, db `db` exists")
        documents = self.collection.find(document)
        self.assertEqual(documents.count(), 1, "Only insert one document")
        self.assertEqual(documents[0].get('id'), document_id, "The document id is the same as the\
                                                              inserted one")
