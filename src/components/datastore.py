import os
import sys
import boto3
import shutil
from zipfile import ZipFile

from src.logger import logging as logger
from src.exception import CustomException

class DataStore:
    def __init__(self):
        self.root = os.path.join(os.getcwd(), "data")
        self.zip = os.path.join(self.root, "archive.zip")
        self.images = os.path.join(self.root, "caltech-101")
        self.list_unwanted = ["BACKGROUND_Google"]
        self.s3 = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY") ).client('s3')
        self.bucket_name=os.getenv("AWS_BUCKET_NAME")
        self.s3_prefix = 'images/'

    def prepare_data(self):
        try:
            logger.info(f" Extracting Data to {self.root}")
            with ZipFile(self.zip, 'r') as files:
                files.extractall(path=self.root)

            files.close()
            logger.info(" Process Completed ")
        except Exception as e:
            message = CustomException(e, sys)
            return {"Created": False, "Reason": message.error_message}

    def remove_unwanted_classes(self):
        try:
            logger.info(" Removing unwanted classes ")
            for label in self.list_unwanted:
                path = os.path.join(self.images,label)
                shutil.rmtree(path, ignore_errors=True)
            logger.info(" Process Completed ")
        except Exception as e:
            message = CustomException(e, sys)
            return {"Created": False, "Reason": message.error_message}

    def sync_data(self):
        try:
            logger.info("\n====================== Starting Data sync ==============================\n")
            for root, dirs, files in os.walk(self.images):
                for filename in files:
                    local_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(local_path, self.images)
                    s3_path = os.path.join(self.s3_prefix, relative_path)
                    self.s3.upload_file(local_path, self.bucket_name, s3_path)
            logger.info("\n====================== Data sync Completed ==========================\n")

        except Exception as e:
            message = CustomException(e, sys)
            return {"Created": False, "Reason": message.error_message}

    def upload_and_sync_data(self):
        try:
            self.prepare_data()
            self.remove_unwanted_classes()
            self.sync_data()
            return True
        except Exception as e:
            message = CustomException(e, sys)
            return {"Created": False, "Reason": message.error_message}


if __name__ == "__main__":
    store = DataStore()
    store.upload_and_sync_data()