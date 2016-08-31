from .base_dbhelper import BaseDB, BaseCollection


class MongoDB(BaseDB):

    def __init__(self, host, db_name, port=27017):
        super().__init__(host, db_name, port)

    def get_collection_names(self):
        pass

    def get_collection(self, name):
        pass


class MongoCollection(BaseCollection):

    def __init__(self, db, name):
        super().__init__(db, name)

    def insert_one(self, document):
        pass

    def insert_many(self, documents):
        """Insert an iterable of documents
        """
        pass

    def delete_one(self, document_id):
        """Delete a single document according to its id
        """
        pass

    def delete_many(self, query):
        """Delete all documents find by the query
        """
        pass

    def delete_one_then_return(self, document_id, projection=None):
        """Deletes a single document then return the document.
        """
        pass

    def update_one(self, document_id, update_data, upsert=False):
        """Update a single document according to its id
        """
        pass

    def update_one_then_return(self, document_id, update_data, upsert=False, projection=None):
        """Update a single document then return the updated document
        """
        pass

    def update_many(self, query, update_data, upsert=False):
        """Update all documents find by the query
        """
        pass

    def find(self, query, projection, limit=None, skip=None, sort=None):
        """Find documents according to the query
        """
        pass

    def find_one(self, query):
        """Find one document aoccording to the query
        """
        pass

    def count(self, query, limit=None, skip=None):
        """Get the number of documents in this collection.
        """
        pass

    def distinct(self, query):
        """Get a list of distinct values for key among all documents in this collection.
        """
        pass

    def create_index(self, keys, **kwargs):
        """Creates an index on this collection.
        """
        pass

    def drop_index(self, index_name):
        """Drops the specified index on this collection.
        """
        pass

    def reindex(self):
        """Rebuilds all indexes on this collection.
        """
        pass

    def get_index_names(self):
        """Get the list of index name
        """
        pass

    def get_index(self, name):
        """Get a index according to its name
        """
        pass
