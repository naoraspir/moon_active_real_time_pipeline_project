import json
import numpy as np
import pandas as pd
import pytest
from src.data_extractor import DataExtractor

#create a mock class for the kafka message
class MockMsg:
    def __init__(self, value):
        self.value = value

    def value(self):
        return self.value

@pytest.fixture
def mock_kafka_consumer()-> list[MockMsg]:
    '''This function creates a mock Kafka consumer'''
    # make a collection of massages to mock the kafka stream 
    messages = [
        MockMsg(json.dumps({
        'user_id': np.random.randint(1, 1000),
        'last_page_1': np.random.randint(1, 10),
        'last_page_2': np.random.randint(1, 10),
        'last_page_3': np.random.randint(1, 10),
        'time_spent_1': np.random.randint(1, 100),
        'time_spent_2': np.random.randint(1, 100),
        'time_spent_3': np.random.randint(1, 100)
        })) for i in range(1000)
    ]
    return messages

@pytest.fixture
def mock_kafka_consumer_invalid():
    '''This function creates a mock Kafka consumer with invalid data'''
    # make a collection of massages to mock the kafka stream 
    messages = None
    return messages

def mock_user_ids():
    '''This function creates a mock user ids'''
    # make a collection of user ids to mock the user ids from MySQL uniquly
    user_ids = [np.random.randint(1, 1000) for i in range(1000)]
    return user_ids

async def mock_mysql_dataframe(query, con):
    '''This function creates a mock MySQL connection'''
    n_samples = 1000
    # make a dataframe to mock the return value from pd.read_sql 
    lst = mock_user_ids()
    offline_data = pd.DataFrame({
        'user_id': lst,
        'total_purchases': np.random.randint(1, 10, n_samples),
        'total_amount_spent': np.random.uniform(1, 1000, n_samples),
        'average_order_value': np.random.uniform(1, 100, n_samples),
        'days_since_last_purchase': np.random.randint(1, 30, n_samples),
        'is_returning_customer': np.random.choice([True, False], n_samples)
    })
    return offline_data

async def mock_mysql_dataframe_invalid():
    '''This function creates a mock MySQL connection with invalid data'''
    # make a dataframe to mock the return value from pd.read_sql 
    offline_data = None
    return offline_data

def create_mock_data_extractor(mock_kafka_consumer, monkeypatch: pytest.MonkeyPatch):
    '''This function creates a mock data extractor'''
    monkeypatch.setattr(pd, 'read_sql', mock_mysql_dataframe)
    data_extractor = DataExtractor(
        kafka_consumer=mock_kafka_consumer,
        offline_db_conn=None
    )
    return data_extractor

@pytest.mark.asyncio
async def test_extract_kafka_data(mock_kafka_consumer, monkeypatch: pytest.MonkeyPatch):
    '''This function tests the extract_kafka_data function'''
    try:
        # create a mock data extractor
        data_extractor = create_mock_data_extractor(mock_kafka_consumer, monkeypatch)
        # extract data
        data = await data_extractor.extract_kafka_data(batch_size= 500)
        # check that the data is a pandas dataframe
        print(data.head())
        assert isinstance(data, pd.DataFrame)
        # check that the data has the correct number of rows
        assert len(data) == 500
        # check that the data contains the correct columns
        columns = [
                'user_id',
                'last_page_1',
                'last_page_2', 
                'last_page_3', 
                'time_spent_1', 
                'time_spent_2', 
                'time_spent_3', 
                ]
        for column in columns:
            assert column in data.columns
    except Exception as error_message:
        raise error_message

@pytest.mark.asyncio
async def test_extract_offline_data(monkeypatch: pytest.MonkeyPatch):
    '''This function tests the extract_offline_data function'''
    try:
        # create a mock data extractor
        data_extractor = create_mock_data_extractor(None, monkeypatch)
        # extract data
        data = await data_extractor.extract_offline_data(mock_user_ids())
        # check that the data is a pandas dataframe
        assert isinstance(data, pd.DataFrame)
        # check that the data has the correct number of rows
        assert data.shape[0] == 1000
        # check that the data contains the correct columns
        columns = [
                'total_purchases',
                'total_amount_spent', 
                'average_order_value', 
                'days_since_last_purchase', 
                'is_returning_customer',
                ]
        for column in columns:
            assert column in data.columns
    except Exception as error_message:
        raise error_message

@pytest.mark.asyncio
async def test_extract_data(mock_kafka_consumer, monkeypatch: pytest.MonkeyPatch):
    '''This function tests the extract_data function'''
    try:
        # create a mock data extractor
        data_extractor = create_mock_data_extractor(mock_kafka_consumer, monkeypatch)
        # extract data
        data = await data_extractor.extract_data(batch_size= 500)
        # check that the data is a pandas dataframe
        assert isinstance(data, pd.DataFrame)
        # check that the data contains the correct columns
        columns = [
                'user_id',
                'last_page_1',
                'last_page_2', 
                'last_page_3', 
                'time_spent_1', 
                'time_spent_2', 
                'time_spent_3', 
                'total_purchases', 
                'total_amount_spent', 
                'average_order_value', 
                'days_since_last_purchase', 
                'is_returning_customer',
                ]
        for column in columns:
            assert column in data.columns
    except Exception as error_message:
        raise error_message

@pytest.mark.asyncio
async def test_extract_data_invalid():
    '''This function tests the extract_data function with invalid data'''
    with pytest.raises(Exception):
        # create a mock data extractor
        data_extractor = DataExtractor(None, None)
        # extract data
        await data_extractor.extract_data(batch_size= 500)
