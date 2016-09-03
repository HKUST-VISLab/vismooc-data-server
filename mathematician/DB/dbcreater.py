from . import mongo_dbhelper
import json

def db_creater(db_configfile="./dbconfig.cfg"):
    with open(db_configfile, 'r') as db_config:
        config = json.load(db_config)
    
    db = mongo_dbhelper.MongoDB("localhost", config.db_name)
    db.add_user(config.db_user, config.db_passwd)
    for collection in config.db_collections:
        db.get_collection(collection.name)


    
    
