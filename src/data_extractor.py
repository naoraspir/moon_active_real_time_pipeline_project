# a class to extract data both from the kafka stream and the offline database
import json
import logging
import pandas as pd

from consts.paths_and_numbers import REAL_TIME_COLS


class DataExtractor:
    '''This class is used to extract data from the kafka stream and the offline database'''

    def __init__(self, kafka_consumer, offline_db_conn):
        self.kafka_consumer = kafka_consumer
        self.offline_db_conn = offline_db_conn
        self.version = "1.0.0"

    async def extract_data(self, batch_size=100) -> pd.DataFrame:
        '''This function extracts data from the kafka stream and the offline database'''
        try:
            # try to exttract data
            logging.info('Loading data')
            # load extract from kafka stream
            kafka_data = await self.extract_kafka_data(batch_size)
            # get relevant user ids for kafka data
            user_ids_list = kafka_data['user_id'].tolist()
            # load data from offline database
            offline_data = await self.extract_offline_data(user_ids_list)
            # concatenate the data
            data = kafka_data.merge(offline_data, on='user_id')
            return pd.DataFrame(data)
        except Exception as error_message:
            # log error
            logging.error(error_message)
            raise error_message

    async def extract_kafka_data(self, batch_size) -> pd.DataFrame:
        '''This function extract data from the kafka stream'''
        try:
            # try to extract data
            logging.info("extracting data from kafka stream")
            # iterate over the messages in the kafka stream and compile a batch of real-time data
            kafka_data = pd.DataFrame()
            for msg in self.kafka_consumer:
                row = pd.json_normalize(json.loads(msg.value))
                if not all(col_name in row.columns for col_name in REAL_TIME_COLS):
                    raise ValueError(
                        "The kafka stream is missing one or more of the following columns: {}".format(REAL_TIME_COLS))
                # make sure that user_id is an integer
                row['user_id'] = row['user_id'].astype(int)
                if kafka_data.empty:
                    kafka_data = row
                else:
                    kafka_data = kafka_data.append(row)
                if len(kafka_data) == batch_size:
                    break
            return kafka_data
        except Exception as error_message:
            # log error
            logging.error(error_message)
            raise error_message

    async def extract_offline_data(self, user_ids_list) -> pd.DataFrame:
        '''This function extract data from the offline database for the users in the user_ids_list'''
        try:
            # try to extract data
            logging.info("extracting data from offline database")
            # create a query to the offline database
            query = "SELECT user_id, total_purchases, total_amount_spent, average_order_value, days_since_last_purchase, is_returning_customer FROM user_features WHERE user_id IN ({})".format(
                ','.join([str(x) for x in user_ids_list]))
            # execute the query
            df = await pd.read_sql(query, con=self.offline_db_conn)
            # make sure that user_id is an integer for the entire dataframe
            df['user_id'] = df['user_id'].astype(int)
            return df
        except Exception as error_message:
            # log error
            logging.error(error_message)
            raise error_message
