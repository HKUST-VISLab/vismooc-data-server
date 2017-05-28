'''Test mongodb_helper module
'''
import unittest
import copy
import pymongo
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

    def test_get_db(self):
        '''test get database instance
        '''
        self.assertIsInstance(self.mongodb.get_db(), pymongo.database.Database,
                              "db should be an instance of pymongo.database.Database")

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
        wrong_db = "foo"
        wrong_name = 123
        with self.assertRaises(TypeError, msg="db should be a instance \
                               of pymongo.database.Database"):
            MongoCollection(wrong_db, "asdf")
        with self.assertRaises(TypeError, msg="name should be a instance of str"):
            MongoCollection(self.mongodb.get_db(), wrong_name)
        database = self.mongodb.get_db()
        collection = MongoCollection(database, "test_collection")
        self.assertIsInstance(collection, MongoCollection, "New a MongoCollection instance")

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

    def test_insert_many(self):
        '''Test the insert_many()
        '''
        document_id_1 = "asdfsdf"
        document_1 = {'id':document_id_1}
        document_id_2 = "gregrg"
        document_2 = {'id':document_id_2}
        documents = [document_1, document_2]
        self.collection.insert_many(copy.deepcopy(documents))
        client = MongoClient("127.0.0.1", 27017)
        db_names = client.database_names()
        self.assertIn(self.db_name, db_names, "After insert_many, db `db` exists")
        query_str = {'$or' : documents}
        documents_find = self.collection.find(query_str)
        self.assertEqual(documents_find.count(), 2, "Inserted 2 document")
        self.assertEqual([{'id': value['id']} for value in documents_find], documents,
                         "The documents should be the same as the inserted ones")

    def test_delete_one(self):
        '''Test the delete_one()
        '''
        document_id = "asdfsdf"
        document = {'id':document_id}
        self.collection.insert_one(document)
        document_find = self.collection.find_one(document)
        self.assertEqual(document_find.get('id'), document_id,
                         "Before delete_one, the document exists")
        self.collection.delete_one(document_find.get("_id"))
        document_find = self.collection.find_one(document)
        self.assertIsNone(document_find, "After delete_one, the document should not exist")

    def test_delete_many(self):
        '''Test the delete_many()
        '''
        document_id_1 = "asdfsdf"
        document_1 = {'id':document_id_1}
        document_id_2 = "gregrg"
        document_2 = {'id':document_id_2}
        documents = [document_1, document_2]
        self.collection.insert_many(copy.deepcopy(documents))
        query_str = {'$or' : documents}
        documents_find = self.collection.find(query_str)
        self.assertEqual(documents_find.count(), 2,
                         "Before delete_many, the documents exist")
        self.collection.delete_many(query_str)
        documents_find = self.collection.find(query_str)
        self.assertEqual(documents_find.count(), 0,
                         "After delete_many, the documents should not exist")

    def test_delete_one_then_return(self):
        '''Test the delete_one_then_return()
        '''
        document_id_1 = "asdfsdf"
        document_1 = {'id':document_id_1}
        document_id_2 = "gregrg"
        document_2 = {'id':document_id_2}
        documents = [document_1, document_2]
        self.collection.insert_many(documents)
        query_str = {'$or' : documents}
        documents_find = self.collection.find(query_str)
        document_0_id = documents_find[0].get("id")
        self.assertEqual(documents_find.count(), 2,
                         "Before delete_one_then_return, there are 2 documents")
        deleted_document = self.collection.delete_one_then_return(documents_find[0].get("_id"))
        documents_find_2 = self.collection.find(query_str)
        self.assertEqual(documents_find_2.count(), 1,
                         "After delete_one_then_return, there should be 1 document left")
        document_compare = document_1 if document_0_id == document_id_1 else document_2
        self.assertEqual(deleted_document, document_compare,
                         "The returned document should be one of the original documents")

    def test_update_one(self):
        '''Test the update_one()
        '''
        document_id = "asdfsdf"
        document = {'id':document_id}
        self.collection.insert_one(document)
        document_find = self.collection.find_one(document)
        self.assertEqual(document_id, document_find.get("id"),
                         "Before update, the `id` value should be same as the original one")
        document_id_new = "oeuoehf"
        self.collection.update_one(document, {'$set': {'id': document_id_new}})
        document_find = self.collection.find_one({'id': document_id_new})
        self.assertEqual(document_id_new, document_find.get("id"),
                         "After update, the `id` value should be same as the new one")

    def test_update_one_then_return(self):
        '''Test the update_one_then_return()
        '''
        document_id = "asdfsdf"
        document = {'id':document_id}
        self.collection.insert_one(document)
        document_find = self.collection.find_one(document)
        self.assertEqual(document_id, document_find.get("id"),
                         "Before update, the `id` value should be same as the original one")
        document_id_new = "oeuoehf"
        document_returned = self.collection.update_one_then_return(
            document_find.get('_id'), {'$set': {'id': document_id_new}})
        document_find = self.collection.find_one({'id':document_id_new})
        self.assertEqual(document_id_new, document_find.get("id"),
                         "After update, the `id` value should be same as the new one")
        document_returned.pop("_id", None)
        self.assertEqual(document_returned, {'id': document_id_new},
                         "The returned document should be the same as the new document")

    def test_update_many(self):
        '''Test the update_many
        '''
        document_id_1 = "asdfsdf"
        document_1 = {'id':document_id_1}
        document_id_2 = "gregrg"
        document_2 = {'id':document_id_2}
        documents = [document_1, document_2]
        query_update = {'$or' : documents}
        document_id_new = 'find_this'
        query_str = {'id': document_id_new}
        self.collection.insert_many(documents)
        documents_find = self.collection.find(query_str)
        origin_count = documents_find.count()
        self.collection.update_many(query_update, {'$set': {'id': document_id_new}})
        documents_find = self.collection.find(query_str)
        self.assertEqual(documents_find.count(), origin_count+2,
                         "After update_many, there should be 2 more documents satisfied")

    def test_find(self):
        '''Test the find()
        '''
        documents = [{'id': number} for number in range(10)]
        self.collection.insert_many(documents)
        query_str = {}
        documents_find = self.collection.find(query_str)
        self.assertEqual(documents_find.count(), 10,
                         "Should find all document if given empty query")
        documents_find = self.collection.find(query_str, limit=5)
        self.assertLessEqual(documents_find.count(with_limit_and_skip=True), 5,
                             "At most 5 element due to the limitation")
        documents_find = self.collection.find(query_str, skip=1)
        self.assertEqual(documents_find.count(with_limit_and_skip=True), 9,
                         "Should return 9 element due to the skip")
        query_str = {'id': {'$gt': 5}}
        documents_find = self.collection.find(query_str)
        self.assertEqual(documents_find.count(), 4,
                         "There should be 4 element greater than 5")

    def test_find_one(self):
        '''Test the find_one()
        '''
        documents = [{'id': number} for number in range(10)]
        self.collection.insert_many(documents)
        query_str = {}
        documents_find = self.collection.find_one(query_str)
        self.assertIsNotNone(documents_find,
                             "At most 1 element returned")
        query_str = {'id': 5}
        documents_find = self.collection.find_one(query_str)
        self.assertIsNotNone(documents_find,
                             "Should return the exact one")
        self.assertEqual(documents_find.get('id'), 5,
                         "Should find the exact one")

    def test_count(self):
        '''Test the count()
        '''
        documents = [{'id': number} for number in range(10)]
        self.collection.insert_many(documents)
        query_str = {}
        count = self.collection.count(query_str)
        self.assertEqual(count, 10,
                         "Count should be 10")
        self.collection.delete_many({'id': 9})
        count = self.collection.count(query_str)
        self.assertEqual(count, 9,
                         "After one deletion, count should be 9")
        query_str = {'id': {'$lt': 4}}
        count = self.collection.count(query_str)
        self.assertEqual(count, 4,
                         "Count should be 4 after applied the condition")

    def test_distinct(self):
        '''Test the distinct()
        '''
        documents = [{'id': number, 'other': 'same'} for number in range(10)]
        self.collection.insert_many(documents)
        query_str = {}
        distinct_values = self.collection.distinct('id', query_str)
        self.assertEqual(len(distinct_values), 10,
                         "There should be 10 different ids")
        distinct_values = self.collection.distinct('other', query_str)
        self.assertEqual(len(distinct_values), 1,
                         "There should be 1 distinct others")
        query_str = {'id': {'$lt': 4}}
        distinct_values = self.collection.distinct('id', query_str)
        self.assertEqual(len(distinct_values), 4,
                         "There should be 4 different ids when apply the condition")

    def test_create_index(self):
        '''Test the create_index()
        '''
        documents = [{'id': number} for number in range(10)]
        self.collection.insert_many(documents)
        collection = MongoClient()[self.db_name][self.collection_name]
        count_before = len(collection.index_information().keys())
        self.collection.create_index('id')
        count_after = len(collection.index_information().keys())
        self.assertEqual(count_before+1, count_after,
                         "There should be one more index")

    def test_drop_index(self):
        '''Test the drop_index()
        '''
        documents = [{'id': number} for number in range(10)]
        self.collection.insert_many(documents)
        self.collection.create_index('id', name='my_id')
        collection = MongoClient()[self.db_name][self.collection_name]
        count_before = len(collection.index_information().keys())
        self.collection.drop_index('my_id')
        count_after = len(collection.index_information().keys())
        self.assertEqual(count_before-1, count_after,
                         "After drop index, there should be one less index")














