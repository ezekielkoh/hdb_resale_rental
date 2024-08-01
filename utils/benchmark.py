from typing import Callable
from functools import wraps
from time import perf_counter

import pandas as pd

def perf(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = perf_counter()
        result = func(*args, **kwargs)
        end = perf_counter()
        print(f"{func.__name__} took {end - start:.3f} seconds")
        return result
    return wrapper

# def save_pd(df: pd.DataFrame, file_name:str) -> None:
#     """ 
#     Save pandas dataframe to multiple formats:
#         - csv
#         - parquet
#         - delta lake
#         - json
#     """
#     from deltalake.writer import write_deltalake
#     import pandas as pd

#     df.to_csv(f'../data/{file_name}.csv', index=False)
#     df.to_parquet(f'../data/{file_name}.parquet', index=False)
#     df.to_json(f'../data/{file_name}.json', orient='records', lines=True)

#     write_deltalake(f'../data/{file_name}', df)

#     print(f"CSV, Parquet, JSON, and Delta Lake files saved to ../data/{file_name}")

# def load_data(file_name:str, method:str) -> pd.DataFrame:
#     """ 
#     Load data from multiple formats:
#         - csv
#         - parquet
#         - delta lake
#         - json
#     """
#     from deltalake import DeltaTable
#     if method == 'csv':
#         data = pd.read_csv(f'../data/{file_name}.csv')
#     elif method == 'parquet':
#         data = pd.read_parquet(f'../data/{file_name}.parquet')
#     elif method == 'deltalake':
#         dt = DeltaTable(f'../data/{file_name}')
#         data = dt.to_pyarrow_table().to_pandas()
#     elif method == 'json':
#         data = pd.read_json(f'../data/{file_name}.json', lines=True)
#     else:
#         raise ValueError('Invalid method')
#     return data

# def benchmark_load_performance(file_name:str):
#     benchmark_methods = ['csv', 'parquet', 'deltalake', 'json']
#     from collections import defaultdict
#     results = defaultdict(list)
#     for method in benchmark_methods:
#         print(f'Benchmarking {method} load performance')
#         start_time = perf_counter()
#         load_data(file_name, method)
#         end_time = perf_counter()
#         elapsed_time = end_time - start_time
#         results['method'].append(method)
#         results['elapsed_time'].append(elapsed_time)
#         results['read_method'] = 'pandas'
#     return pd.DataFrame(results)
