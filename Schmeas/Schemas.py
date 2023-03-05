# These classes are used to validate the request and response data by FastAPI and Pydantic
from pydantic import BaseModel

class PredictionRequest(BaseModel):
    '''This class is used to validate the request data'''
    batch_size: int

class PredictionResponse(BaseModel):
    '''This class is used to validate the response data'''
    users_predictions: str
    user_ids_corrupt_or_missing_data: list
    request_id: int

# This class is to define a schema for data resource configuration
class DataResourceConfig(BaseModel):
    '''This class is used to validate the data resource configuration'''
    kafka_config: str
    mysql_config: str