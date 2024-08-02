from utils.io import (fetch_data,
                      read_config)
from utils.io import read_config, save_data, config_logger


if __name__ == '__main__':
    logger = config_logger('pipeline', file_path='logs/pipeline.log')
    logger.info('Running script')
    rental_config = read_config(key='rental')
    resale_config = read_config(key='resale')

    resale_df = fetch_data(**resale_config, refresh_data='resale')
    rental_df = fetch_data(**rental_config, refresh_data='rental')
    logger.info('Script completed')