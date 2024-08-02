from utils.io import (fetch_data,
                      read_config,
                      format_rental_df, 
                      format_resale_df)
from utils.io import read_config, save_data



if __name__ == '__main__':
    rental_config = read_config(key='rental')
    resale_config = read_config(key='resale')

    resale_df = fetch_data(**resale_config, refresh_data='resale.parquet')
    rental_df = fetch_data(**rental_config, refresh_data='rental.parquet')
    save_data(resale_df, 'test')
    print('works')