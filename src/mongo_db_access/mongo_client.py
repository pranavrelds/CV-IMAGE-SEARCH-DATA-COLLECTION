import os
import pymongo
from dotenv import load_dotenv
load_dotenv() 


class MongodbClient:
    client = None

    def __init__(self, database_name=os.environ["DATABASE_NAME"]) -> None:
        if MongodbClient.client is None:
            MongodbClient.client = pymongo.MongoClient(os.getenv("MONGO_DB_URL"))
        self.client = MongodbClient.client
        self.database = self.client[database_name]
        self.database_name = database_name