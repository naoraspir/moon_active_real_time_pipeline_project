# a class for Extract Transform and Predict pipeline orchestration
import logging

class ETPPipeline:
    '''This class is used to orchestrate the ETP pipeline'''
    def __init__(self, data_transformer, data_predictor):
        '''This function initializes the ETP pipeline'''
        self.data_extractor = None
        self.data_transformer = data_transformer
        self.data_predictor = data_predictor
        self.version = "1.0.0"

    def set_data_extractor(self, data_extractor):
        '''This function sets the data extractor'''
        self.data_extractor = data_extractor

    async def run(self, batch_size=100):
        '''This function runs the ETP pipeline'''
        try:
            # try to run the ETP pipeline
            logging.info("Running ETP pipeline")
            # extract data
            data = await self.data_extractor.extract_data(batch_size)
            # transform data
            processed_data, user_ids_for_prediction, corrupt_data_user_ids = self.data_transformer.transform_data(
                data)
            # predict on data
            predictions = self.data_predictor.batch_predict(processed_data)
            result_dict = dict(zip(user_ids_for_prediction, predictions))
            
            return result_dict, corrupt_data_user_ids
        except Exception as error_message:
            # log error
            logging.error(error_message)
            return None
