'''
IO Module to manage input and output operations
'''

from ruamel.yaml import YAML
import pandas as pd
import requests 
from utils.benchmark import perf
import os
import logging

def read_config(key: str = None) -> dict:
    """ 
    Parses in configuration file and returns result as a dictionary. 

    Args:
    key: str
        key to access specific configuration

    Returns:
    dict
        dictionary containing configuration
    """
    with open('./conf/config.yml', 'r') as rf:
        yaml = YAML(typ='safe')
        config = yaml.load(rf)
    
    return config if key is None else config[key]


@perf
def fetch_data(url: str, params: dict, refresh_data: str = None) -> pd.DataFrame:
    resp = requests.get(url=url, params=params)
    # raise error if status code is not 200
    resp.raise_for_status()

    # check if refresh_data file exists
    if refresh_data:
        if refresh_data+'.parquet' not in os.listdir('./data'):
            raise FileNotFoundError(f'{refresh_data} not found in data folder')
        
        print('Merging Records...')
        current_df = pd.read_parquet(f'./data/{refresh_data}.parquet')
        new_df = pd.DataFrame(resp.json()['result']['records'])
        if refresh_data.__contains__('resale'):
            new_df = format_resale_df(new_df)
        elif refresh_data.__contains__('rental'):
            new_df = format_rental_df(new_df)
        combined_df = pd.concat([current_df, new_df])
        save_data(combined_df, refresh_data)
        return combined_df.drop_duplicates().reset_index(drop=True)
    return pd.DataFrame(resp.json()['result']['records'])

    
def format_resale_df(df: pd.DataFrame) -> pd.DataFrame:
    """ 
    Cleans the data and format the data for exploration. The following actions are done:
    - Convert 'month' column to datetime format
    - Drop duplicates
    - converts 'remaining_lease' column to months 
    """

    # convert to datetime format
    cleaned_df = df.copy()
    cleaned_df['month'] = pd.to_datetime(cleaned_df['month'], format='%Y-%m').dt.strftime('%Y-%m-%d')
    cleaned_df['floor_area_sqm'] = cleaned_df['floor_area_sqm'].astype(float)
    cleaned_df['lease_commence_date'] = cleaned_df['lease_commence_date'].astype(int)
    cleaned_df['resale_price'] = cleaned_df['resale_price'].astype(float)

    # convert remaining lease to months
    cleaned_df[['remaining_years', 'remaining_months']] = cleaned_df['remaining_lease'].str.extract(r'^(\d+)[A-Za-z\s]*(\d*)')
    cleaned_df['remaining_years'] = cleaned_df['remaining_years'].astype(int)
    cleaned_df['remaining_months'] = cleaned_df['remaining_months'].replace('', 0).astype(int)
    cleaned_df['remaining_lease_months'] = cleaned_df['remaining_years']*12 + cleaned_df['remaining_months']

    return cleaned_df.drop(columns=['_id']).sort_values('month', ascending=True)

def format_rental_df(df: pd.DataFrame) -> pd.DataFrame:
    # convert to datetime format
    cleaned_df = df.copy()
    cleaned_df['rent_approval_date'] = pd.to_datetime(cleaned_df['rent_approval_date'], format='%Y-%m').dt.strftime('%Y-%m-%d')
    return cleaned_df.drop(columns=['_id']).sort_values('rent_approval_date', ascending=True)

def save_data(df: pd.DataFrame, file_name: str) -> None:
    """ 
    Save data to multiple formats:
        - csv
        - parquet
    """
    df.to_csv(f'./data/{file_name}.csv', index=False)
    df.to_parquet(f'./data/{file_name}.parquet', index=False)
    print(f"CSV and Parquet files saved to ../data/{file_name}")

def config_logger(name, file_path=None, streaming=None, level=logging.INFO):
    '''
    Configures logger for logging
    '''
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if file_path:
        file_handler = logging.FileHandler(file_path)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    if streaming:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger