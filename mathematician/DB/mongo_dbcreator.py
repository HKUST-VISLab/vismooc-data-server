import json
from .mongo_dbhelper import MongoDB

def db_creater(db_configfile="./dbconfig.json"):
    with open(db_configfile, 'r') as db_config:
        config = json.load(db_config)
    
    db = MongoDB(config.host, config.db_name)
    db.add_user(config.db_user, config.db_passwd)
    for collection in config.db_collections:
        db.get_collection(collection.name)
        print(collection.name)
