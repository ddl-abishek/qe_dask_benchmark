import dask.dataframe as dd
import pandas as pd
import dask
from dask.distributed import Client, performance_report
from utils import convert_date
import yaml
from datetime import datetime
import argparse
import os

config = yaml.load(open("config.yml", "r"), yaml.SafeLoader)

def preprocess_csv(csv_year):
    if '2015' in csv_year:
        dtype = {'Vehicle Expiration Date' : object,
                 'Violation Precinct' : float,
                 'Issuer Precinct' : float,
                 'Vehicle Year' : float}
    else:
        dtype = {'Vehicle Expiration Date' : float,
                 'Violation Precinct' : float,
                 'Issuer Precinct' : float,
                 'Vehicle Year' : float}        
    
    clean_data = dd.read_csv(config['Dataset'][csv_year], 
                             usecols=['Vehicle Expiration Date', 'Violation Precinct', 'Issuer Precinct', 'Vehicle Year'], 
                             dtype=dtype).dropna()
    
    clean_data['Vehicle Expiration Date'] = clean_data['Vehicle Expiration Date'].apply(convert_date, meta=('Vehicle Expiration Date', 
                                                                                                            dtype['Vehicle Expiration Date']))
    
    if not(os.path.isdir(config['clean_data']['dir'])):
        os.makedirs(config['clean_data']['dir'])

    clean_data.compute().to_csv(config['clean_data'][csv_year], index=False)

if __name__ == "__main__":
    service_port = os.environ['DASK_SCHEDULER_SERVICE_PORT']
    service_host = os.environ['DASK_SCHEDULER_SERVICE_HOST']

    client = Client(address=f'{service_host}:{service_port}', direct_to_workers=True)
    client.wait_for_workers(n_workers=23)
    client.restart()
    client.upload_file('utils.py')
    
    with performance_report(filename=f"{config['artifacts']['path']}/dask-report_preprocess_{str(datetime.now())}.html"):
        dask_map = client.map(preprocess_csv, ['2013-14',
                                              '2013-14_1',
                                              '2013-14_2',
                                              '2013-14_3',
                                              '2013-14_4',
                                              '2015',
                                              '2015_1',
                                              '2015_2',
                                              '2015_3',
                                              '2015_4',
                                              '2016',
                                              '2016_1',
                                              '2016_2',
                                              '2016_3',
                                              '2016_4',
                                              '2017', 
                                              '2017_1',
                                              '2017_2',
                                              '2017_3',
                                              '2017_4'])
        client.gather(dask_map)
