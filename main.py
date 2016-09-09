import json
import pymongo
from mathematician.DB.mongo_dbhelper import MongoDB


def db_creater(db_configfile="./dbconfig.json"):
    with open(db_configfile, 'r') as db_config:
        config = json.load(db_config)
    
    db = MongoDB("localhost", config['db_name'])
    db.clear()

    # There is some trouble here
    # db.add_user(config['db_user'], config['db_passwd'])
    for collection in config['db_collections']:
        
        #first construct the validator
        validation_list = [] 
        if collection["fields"]:
            for a_field in collection["fields"]:
                validation_list.append({a_field["field_name"] : a_field["validation"]})
        
        #create collection with specified validation option
        a_collection = db.create_collection(collection['collection_name'], validator={"$and":validation_list})
            

        #second make the index
        if collection["index"]:
            indexes = []
            for a_index in collection["index"]:
                tmp_index = (a_index["field"], a_index["order"])
                indexes.append(tmp_index)
            a_collection.create_index(indexes)
                

if __name__ == '__main__':
    db_creater()
    print("Successfully created the database")
