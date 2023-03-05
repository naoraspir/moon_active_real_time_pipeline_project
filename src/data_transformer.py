# a class for transforming the data from the real-time stream and the offline database
import logging
import pandas as pd
import numpy as np


class DataTransformer:
    '''This class is used to transform the data from the kafka stream and the offline database'''
    def __init__(self):
        self.version = "1.0.0"
    
    def transform_data(self, df: pd.DataFrame):
        '''This function ttransform the data'''
        try:
            #try to transform data
            logging.info("transforming data")

            # convert rows with negative or missing time spent values to 0
            df['time_spent_1'] = df['time_spent_1'].apply(lambda x: x if x >= 0 else 0)
            df['time_spent_2'] = df['time_spent_2'].apply(lambda x: x if x >= 0 else 0)
            df['time_spent_3'] = df['time_spent_3'].apply(lambda x: x if x >= 0 else 0)

            # replace any negative or missing values in total_purchases, total_amount_spent, and average_order_value with 0
            df['total_purchases'] = df['total_purchases'].apply(lambda x: x if x >= 0 else 0)
            df['total_amount_spent'] = df['total_amount_spent'].apply(lambda x: x if x >= 0 else 0)
            df['average_order_value'] = df['average_order_value'].apply(lambda x: x if x >= 0 else 0)

            # Replace any negative or missing values in days_since_last_purchase with max value
            max_days_since_last_purchase = df['days_since_last_purchase'].max()
            df['days_since_last_purchase'] = df['days_since_last_purchase'].apply(lambda x: x if x >= 0 else max_days_since_last_purchase)

            # Replace any negative or missing values in is_returning_customer with False
            df['is_returning_customer'] = df['is_returning_customer'].apply(lambda x: x if x else False)

            #drop what could not be filled with estimates above
            df_na = df.dropna(how='any', inplace=False)
            dropped_user_ids = set(df['user_id'].values) - set(df_na['user_id'].values)
            df = df_na

            # Convert is_returning_customer to 0 and 1
            df['is_returning_customer'] = df['is_returning_customer'].apply(lambda x: 1 if x else 0)

            # Convert time spent values to minutes
            df['time_spent_1'] = df['time_spent_1'] / 60
            df['time_spent_2'] = df['time_spent_2'] / 60
            df['time_spent_3'] = df['time_spent_3'] / 60

            # feature extraction ,lightly lol
            df['total_time_spent'] = df['time_spent_1'] + df['time_spent_2'] + df['time_spent_3']
            df['avg_time_spent'] = df['total_time_spent'] / 3
            

            #keep user ids for later but out of the model input
            user_ids = df['user_id'].values
            df = df.drop(['user_id'], axis=1)

            #AND MORE TRANSFORMATIONS HERE

            return df, list(user_ids), dropped_user_ids
        except Exception as error_message:
            #log error
            logging.error(error_message)
            raise error_message