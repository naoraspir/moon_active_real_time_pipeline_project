
import numpy as np
import pandas as pd
import pytest
from consts.paths_and_numbers import MODEL_NAME
from src.data_predictor import PurchasePredictor


@pytest.fixture
def mock_valid_df():
    '''This function creates a mock dataframe'''
    # Generate synthetic data for real-time features
    n_samples = 1000
    rt_data = pd.DataFrame({
        'last_page_1': np.random.randint(1, 10, n_samples),
        'last_page_2': np.random.randint(1, 10, n_samples),
        'last_page_3': np.random.randint(1, 10, n_samples),
        'time_spent_1': np.random.randint(1, 100, n_samples),
        'time_spent_2': np.random.randint(1, 100, n_samples),
        'time_spent_3': np.random.randint(1, 100, n_samples),
    })

    # Generate synthetic data for offline features
    offline_data = pd.DataFrame({
        'total_purchases': np.random.randint(1, 10, n_samples),
        'total_amount_spent': np.random.uniform(1, 1000, n_samples),
        'average_order_value': np.random.uniform(1, 100, n_samples),
        'days_since_last_purchase': np.random.randint(1, 30, n_samples),
        'is_returning_customer': np.random.choice([True, False], n_samples)
    })

    #Generate aggrigated data for rt features
    agg_data = pd.DataFrame({
        'total_time_spent': np.random.randint(1, 300, n_samples),
        'avg_time_spent': np.random.randint(1, 100, n_samples),
    })

    # Merge the two dataframes
    df = pd.concat([rt_data, offline_data, agg_data], axis=1)
    return df


@pytest.fixture
def mock_invalid_df():
    '''This function creates a mock invalid dataframe'''
    df = pd.DataFrame()
    return df


def test_invalid_data_predictor_init():
    '''This test checks that the data predictor raises an error when an invalid model is given'''
    with pytest.raises(FileNotFoundError):
        PurchasePredictor("invalid")


def test_valid_data_predictor_init():
    '''This test checks that the data predictor does not raise an error when a valid model is given'''
    try:
        PurchasePredictor()
        PurchasePredictor(MODEL_NAME)
    except FileNotFoundError:
        pytest.fail("PurchasePredictor raised ValueError unexpectedly!")


def test_batch_predict(mock_valid_df):
    '''This test checks that the batch predict method returns a list of predictions'''
    try:
        data_predictor = PurchasePredictor()
        predictions = data_predictor.batch_predict(mock_valid_df)
        assert isinstance(predictions, list)
    except Exception as error_message:
        raise error_message

def test_batch_predict_invalid(mock_invalid_df):
    '''This test checks that the batch predict method returns None when an invalid dataframe is given'''
    data_predictor = PurchasePredictor()
    with pytest.raises(Exception):
        data_predictor.batch_predict(mock_invalid_df)
