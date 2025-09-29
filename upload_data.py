from src.constant import*

from pymongo.mongo_client import MongoClient
import pandas as pd
import json


client = MongoClient(MONGO_DB_URL)

df = pd.read_csv("notebooks/data/stud.csv")

df = df.drop("Unnamed: 0",axis=1)

json_record = list(json.loads(df.T.to_json()).values())

client[MONGO_DATABASE_NAME][MONGO_COLLECTION_NAME].insert_many(json_record)