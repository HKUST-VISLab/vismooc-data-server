from abc import ABCMeta, abstractmethod


class BaseDB(metaclass=ABCMeta):

    def __init__(self):
        pass

    @abstractmethod
    def get_collection_names(self):
        pass

    @abstractmethod
    def get_collection(self, name):
        pass


class BaseCollection(metaclass=ABCMeta):

    def __init__(self, db, name):
        self.db = db
        self.name = name

    @abstractmethod
    def insert_one(self, document):
        pass

    @abstractmethod
    def insert_many(self, documents):
        pass

    @abstractmethod
    def delete_one(self, document_id):
        pass

    @abstractmethod
    def delete_many(self, query):
        pass

    @abstractmethod
    def delete_one_then_return(self, document_id, projection=None):
        pass

    @abstractmethod
    def update_one(self, document_id, update_data, upsert=False):
        pass

    @abstractmethod
    def update_one_then_return(self, document_id, update_data, upsert=False, projection=None):
        pass

    @abstractmethod
    def update_many(self, query, update_data, upsert=False):
        pass

    @abstractmethod
    def find(self, query, projection, limit=None, skip=None, sort=None):
        pass

    @abstractmethod
    def find_one(self, query):
        pass

    @abstractmethod
    def count(self, query, limit=None, skip=None):
        pass

    @abstractmethod
    def distinct(self, query):
        pass

    @abstractmethod
    def create_index(self, keys, **kwargs):
        pass

    @abstractmethod
    def drop_index(self, index_name):
        pass

    @abstractmethod
    def reindex(self):
        pass

    @abstractmethod
    def get_index_names(self):
        pass

    @abstractmethod
    def get_index(self, name):
        pass

    def drop(self):
        pass

    def rename(self, new_name):
        pass

    def group(self, key, query, **kwargs):
        pass

    def map_reduce(self, map_func, reduce_func, out, **kwargs):
        pass
    
    def inline_map_reduce(self, map_func, reduce_func,**kwargs):
        pass

    def parallel_scan(self, num_cursors):
        pass

    
    def initialize_unordered_bulk_op(self, bypass_document_validation=False):
        pass

    def initialize_ordered_bulk_op(self, bypass_document_validation=False):
        pass
