from typing import List, Union, Any

import uvicorn
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse

from src.mongo_db_access.mongo_client import MongodbClient
from src.components.s3_operations import S3Operations


app = FastAPI(title="data-collection-server")
mongo = MongodbClient()
s3 = S3Operations()