import sys
import os
import numpy as np
import pandas as pd
from pymongo import MongoClient
from zipfile import Path
from src.constant import *
from src.exception import CustomException
from src.logger import logging
from dataclasses import dataclass



@dataclass
class DataIngestionConfig:
  artifacts: str = os.path.join(artifacts)

class DataIngestion:
  def __init__(self):
    self.data_ingestion_config = DataIngestionConfig()

  def export_collection_as_dataframe(self,collection_name,db_name):
    try:
      mongo_client = MongoClient(MONGO_DB_URL)  # defined in constant
      collection = mongo_client[db_name][collection_name]
      df = pd.DataFrame(list(collection.find()))
      if"_id" in df.columns.to_list():
        df = df.drop(columns = ["_id"], axis = 1)
        df.replace({"na":np.nan},inplace = True)

      return df
    except Exception as e:
        raise CustomException(e,sys) from e




  def export_data_into_feature_store_file_path(self) ->pd.DataFrame:
    try:
      raw_file_path = self.data_ingestion_config.artifacts
      os.makedirs(raw_file_path,exist_ok = True)

      student_data = self.export_collection_as_dataframe(
          collection_name = MONGO_COLLECTION_NAME,
          db_name = MONGO_DATABASE_NAME
      )
      feature_store_file_path = os.path.join(raw_file_path,"stud_performance.csv")

      student_data.to_csv(feature_store_file_path,index = False)
      return feature_store_file_path

    except Exception as e:
       raise CustomException(e,sys)

  def initate_data_ingestion(self):

    logging.info("Entered initiated_data_ingestion method of data_integration class")


    try:
      feature_store_file_path = self.export_data_into_feature_store_file_path()

      logging.info("got the data from mongodb")

      logging.info("exited initiate_data_ingestion methos of data ingestion class")


      return feature_store_file_path
    except Exception as e:
       raise CustomException(e,sys) from e

if __name__ =="__main__":
  obj = DataIngestion()
  obj.initate_data_ingestion()


