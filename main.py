from utils.io import (fetch_data,
                      read_config,
                      format_rental_df, 
                      format_resale_df)
from utils.io import read_config, save_data

import logging


if __name__ == '__main__':
    log_file = 'logs/pipeline.log'
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_file)])
    
    logger = logging.getLogger(__name__)
    logger.info('Running script')
    rental_config = read_config(key='rental')
    resale_config = read_config(key='resale')

    resale_df = fetch_data(**resale_config, refresh_data='resale.parquet')
    rental_df = fetch_data(**rental_config, refresh_data='rental.parquet')
    save_data(resale_df, 'test')
    logger.info('Script completed')