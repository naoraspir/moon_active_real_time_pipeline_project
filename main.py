import json
import logging
import os
import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from uvicorn import run
from Schmeas.Schemas import DataResourceConfig, PredictionRequest, PredictionResponse
from src.etp_orchestrator import ETPPipeline
from src.data_extractor import DataExtractor
from src.data_transformer import DataTransformer
from src.data_predictor import PurchasePredictor

# Initialize the FastAPI app
app = FastAPI()
origins = ["*"]
methods = ["*"]
headers = ["*"]

# Enable later integration to API Gateway
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=methods,
    allow_headers=headers
)

# We will follow the requests from when the service is initialized and inform the user
REQUEST_ID_COUNTER = 0

# create the data processor object
data_transformer = DataTransformer()

# initiate a Model object once the service is initialized to not repeat loading the model
data_predictor = PurchasePredictor()

# create the extract transform and predict pipeline orchstrator object
etp_pipeline = ETPPipeline(data_transformer, data_predictor)

# base route
@app.get("/")
async def root():
    return {"message": "Welcome to the MoonActive API!"}
# route for the initialization of the data resources
@app.post("/init_data_resources")
async def init_data_resources(body: DataResourceConfig):
    # Create a kafka consumer and an mysql connection, the stream controling service will send the
    # configs for this specific prediction microservice. that way we can allow scalability.
    try:
        # Load Kafka consumer configuration from JSON formatted string
        kafka_config = json.loads(body.kafka_config)
        # Load MySQL connector configuration from JSON formatted string
        mysql_config = json.loads(body.mysql_config)
    except Exception as error_message:
        logging.error(msg=error_message, exc_info=True)
        raise HTTPException(status_code=400, detail={
                            "message": "Error formating resources configuration: {}".format(str(error_message))})
    # Create Kafka consumer instance
    try:
        consumer = KafkaConsumer(
            kafka_config['topics'],
            bootstrap_servers=kafka_config['bootstrap_servers'],
            group_id=kafka_config['group_id'],
            auto_offset_reset=kafka_config['auto_offset_reset'],
            enable_auto_commit=kafka_config['enable_auto_commit'],
            value_deserializer=lambda m: json.loads(m.decode('ascii'))
        )
    except Exception as error_message:
        logging.error(msg=error_message, exc_info=True)
        raise HTTPException(status_code=400, detail={
                            "message": "Error initializing Kafka: {}".format(str(error_message))})

    
    # Create MySQL connection instance
    try:
        connection = mysql.connector.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database'])
    except Exception as error_message:
        logging.error(msg=error_message, exc_info=True)
        raise HTTPException(status_code=400, detail={
                            "message": "Error initializing Kafka: {}".format(str(error_message))})

    # Set data extractor instance
    etp_pipeline.set_data_extractor(DataExtractor(consumer, connection))

    return {"message": "Kafka and MySQL initialized successfully."}

# route for the batch predictions webhook
@app.post("/predictions_webhook", response_model=PredictionResponse)
async def return_batch_predictions(body: PredictionRequest):
    if not etp_pipeline.data_extractor:
        raise HTTPException(status_code=400, detail={
                            "message": "Data resources not initialized. Please call /init_data_resources first."})
    try:
        # run the ETL pipeline with the batch size from the request
        logging.info("Running ETL pipeline with batch size: {}".format(body.batch_size))
        result_dict, corrupt_data_user_ids = await etp_pipeline.run(body.batch_size)

        # create the response object
        global REQUEST_ID_COUNTER
        response = PredictionResponse(
            request_id=REQUEST_ID_COUNTER,
            predictions=json.dumps(result_dict),
            corrupt_data_user_ids=corrupt_data_user_ids
        )
        REQUEST_ID_COUNTER += 1
        return response
    except Exception as error_message:
        logging.error(msg=error_message, exc_info=True)
        raise error_message

if __name__ == "__main__":
    # We will run the app on the port 5000
    port = int(os.environ.get('PORT', 5000))
    run(app, host="0.0.0.0", port=port)
