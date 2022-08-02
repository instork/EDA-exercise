
# TODO: set Connection pool in pymongo
# https://pymongo.readthedocs.io/en/stable/faq.html#how-does-connection-pooling-work-in-pymongo

import os
from pymongo import MongoClient
import pandas as pd
from typing import Dict

def _conn_db() -> MongoClient:
    """ Connect to MongoDB. """
    from dotenv import load_dotenv
    load_dotenv('../.env')
    user = os.getenv("MONGODB_USER")
    pwd = os.getenv("MONGODB_PWD")
    host = os.getenv("MONGODB_HOST")
    port = os.getenv("MONGODB_PORT")
    client = MongoClient(f'mongodb://{user}:{pwd}@{host}:{port}')
    return client

def list_database_names():
    client = _conn_db()
    db_names = client.list_database_names()
    client.close()
    return db_names

def list_collection_names(db_name:str):
    client = _conn_db()
    collection_names = client[db_name].list_collection_names()
    client.close()
    return collection_names

def df_from_db(db_name:str, collection_name:str, query:Dict = {}) -> pd.DataFrame:
    """ Get dataframe from database """
    client = _conn_db()
    db = client[db_name]
    result_df = pd.DataFrame(list(db[collection_name].find(query)))
    client.close()
    return result_df