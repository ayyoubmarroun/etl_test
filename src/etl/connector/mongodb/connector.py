from etl.connector.base import BaseConnector
from pymongo import MongoClient
import pandas as pd


class MongoConnector(BaseConnector):

    def connect(self, host:str, database: str, user=None, password=None,  port=27017, **kwargs):
        if (user and password):
            mongo_uri = 'mongodb://{}:{}@{}:{}/{}'.format(user, password, host, port, database)
            self._con = MongoClient(mongo_uri)[database]
        else:
            self._con = MongoClient(host, port)[database]
        

    def append_data(self, collection: str, data: pd.DataFrame, **kwargs):
        self._con[collection].insert_many(data.to_dict("records"))

    
    def query(self, collection, query: dict, no_id=True) -> pd.DataFrame:
        cursor = self._con[collection].find(query)
        data = pd.DataFrame(list(cursor))
        if(no_id):
            del data["_id"]
        return data
    
    def query_agg(self, collection: str, query: dict, no_id=True) -> pd.DataFrame:
        cursor = self._con[collection].aggregate(query)
        data = pd.DataFrame.from_records(list(cursor))
        data = pd.concat((pd.json_normalize(data["_id"]), data.drop("_id", axis=1)), axis=1)
        return data
