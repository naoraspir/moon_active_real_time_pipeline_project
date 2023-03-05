
import numpy as np
import pandas as pd
import pytest
from src.data_transformer import DataTransformer

@pytest.fixture
def mock_valid_full_df_to_transform():
    '''This function creates a mock dataframe to clean'''
    # Generate synthetic data for real-time features
    n_samples = 1000
    rt_data = pd.DataFrame({
        'last_page_1': np.random.randint(1, 10, n_samples),
        'last_page_2': np.random.randint(1, 10, n_samples),
        'last_page_3': np.random.randint(1, 10, n_samples),
        'time_spent_1': np.random.randint(1, 100, n_samples),
        'time_spent_2': np.random.randint(1, 100, n_samples),
        'time_spent_3': np.random.randint(1, 100, n_samples)
    })

    # Generate synthetic data for offline features
    offline_data = pd.DataFrame({
        'total_purchases': np.random.randint(1, 10, n_samples),
        'total_amount_spent': np.random.uniform(1, 1000, n_samples),
        'average_order_value': np.random.uniform(1, 100, n_samples),
        'days_since_last_purchase': np.random.randint(1, 30, n_samples),
        'is_returning_customer': np.random.choice([True, False], n_samples)
    })

    # generate user ids dataframe for all samples
    user_ids = pd.DataFrame({
        'user_id': np.random.randint(1, 1000, n_samples)
    })

    # Merge the three dataframes
    df = pd.concat([user_ids,rt_data, offline_data], axis=1)
    return df

@pytest.fixture
def mock_invalid_df_to_transform():
    '''This function creates a mock invalid dataframe to clean'''
    # Generate synthetic data for real-time features
    n_samples = 1000
    rt_data = pd.DataFrame({
        'last_page_1': np.random.randint(1, 10, n_samples),
        'last_page_2': np.random.randint(1, 10, n_samples),
        'time_spent_3': np.random.randint(1, 100, n_samples)
    })

    # Generate synthetic data for offline features
    offline_data = pd.DataFrame({
        'total_purchases': np.random.randint(1, 10, n_samples),
        'total_amount_spent': np.random.uniform(1, 1000, n_samples),
        'days_since_last_purchase': np.random.randint(1, 30, n_samples),
        'is_returning_customer': np.random.choice([True, False], n_samples)
    })

    # generate user ids dataframe for all samples
    user_ids = pd.DataFrame({
        'user_id': np.random.randint(1, 1000, n_samples)
    })

    # Merge the three dataframes
    df = pd.concat([user_ids,rt_data, offline_data], axis=1)
    return df

@pytest.fixture
def mock_valid_df_to_transform_with_nulls():
    '''This function creates a mock data frame with null values'''
    # Generate synthetic data for real-time features and inject null values
    n_samples = 1000
    rt_data = pd.DataFrame({
        'last_page_1': np.random.randint(1, 10, n_samples),
        'last_page_2': np.random.randint(1, 10, n_samples),
        'last_page_3': np.random.randint(1, 10, n_samples),
        'time_spent_1': np.empty(n_samples),
        'time_spent_2': np.random.randint(1, 100, n_samples),
        'time_spent_3': np.random.randint(1, 100, n_samples)
    })

    # Generate synthetic data for offline features
    offline_data = pd.DataFrame({
        'total_purchases': np.random.randint(1, 10, n_samples),
        'total_amount_spent': np.empty(n_samples),
        'average_order_value': np.random.uniform(1, 100, n_samples),
        'days_since_last_purchase': np.random.randint(1, 30, n_samples),
        'is_returning_customer': np.random.choice([True, False], n_samples)
    })


    # generate user ids dataframe for all samples
    user_ids = pd.DataFrame({
        'user_id': np.random.randint(1, 1000, n_samples)
    })

    # Merge the three dataframes
    df = pd.concat([user_ids,rt_data, offline_data], axis=1)
    return df


def test_transform_data(mock_valid_full_df_to_transform):
    '''This test checks that the transform_data method returns a dataframe'''
    try:
        data_transformer = DataTransformer()
        transformed_data , user_ids , dropped_user_ids = data_transformer.transform_data(mock_valid_full_df_to_transform)
        assert isinstance(transformed_data, pd.DataFrame)
        assert isinstance(user_ids, list)
        assert isinstance(dropped_user_ids, set)
        assert len(user_ids) == len(transformed_data)
        assert len(dropped_user_ids) == len(mock_valid_full_df_to_transform) - len(transformed_data)
        # check that 'is_returning_customer' is a 1 or 0 and not a boolean column
        assert transformed_data['is_returning_customer'].isin([0,1]).all()
        columns = [
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
            'total_time_spent',
            'avg_time_spent',
            ]
        for column in columns:
            assert column in transformed_data.columns
        
    except Exception as error_message:
        raise error_message

def test_invalid_transform_data(mock_invalid_df_to_transform):
    '''This test checks that the transform_data method excepts a dataframe with invalid columns'''
    with pytest.raises(Exception):
        data_transformer = DataTransformer()
        data_transformer.transform_data(mock_invalid_df_to_transform)

def test_transform_data_with_nulls(mock_valid_df_to_transform_with_nulls):
    '''this test checks that the null values are imputed'''
    try:
        data_transformer = DataTransformer()
        transformed_data , user_ids , dropped_user_ids = data_transformer.transform_data(mock_valid_df_to_transform_with_nulls)
        assert isinstance(transformed_data, pd.DataFrame)
        assert isinstance(user_ids, list)
        assert isinstance(dropped_user_ids, set)
        assert len(user_ids) == len(transformed_data)
        assert len(dropped_user_ids) == len(mock_valid_df_to_transform_with_nulls) - len(transformed_data)
        assert transformed_data['time_spent_1'].isnull().sum() == 0
        assert transformed_data['total_amount_spent'].isnull().sum() == 0
        columns = [
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
            'total_time_spent',
            'avg_time_spent',
            ]
        for column in columns:
            assert column in transformed_data.columns
        assert transformed_data['time_spent_1'].isnull().sum() == 0
        assert transformed_data['total_amount_spent'].isnull().sum() == 0
    except Exception as error_message:
        raise error_message