import logging
import os
import pickle
from sklearn.linear_model import LinearRegression

from consts.paths_and_numbers import MODEL_NAME

class PurchasePredictor:
    '''This class is used to load the model and predict the purchase probability'''
    def __init__(self, model_name = MODEL_NAME):
        self.version = "1.0.0"
        try:
            path = os.path.join(os.path.dirname(__file__), model_name)
            self.model : LinearRegression = self.load_model(path)
        except FileNotFoundError as error_message:
            logging.error(error_message)
            raise error_message

    def load_model(self, model_path):
        '''This method loads the model from the path'''
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        return model
    
    def batch_predict(self, df):
        '''This method predicts the purchase probability for a batch of users'''
        try:
            #try to predict
            logging.info("Predicting batch")
            predictions = self.model.predict(df)
            return list(predictions)
        except Exception as error_message:
            #log error
            logging.error(error_message)
            raise error_message
