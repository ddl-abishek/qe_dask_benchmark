# Usage
# python preprocess.py --csv_year 2015 ; 2015 can be replace by 2013-14 or 2016 or 2017
import dask.dataframe as dd
import pandas as pd
import dask
from dask.distributed import Client
from utils import convert_date
import yaml
import argparse
import os

dask.config.get("distributed.client")

config = yaml.load(open("config.yml", "r"), yaml.SafeLoader)

def convert_date(date): 
    date = str(date)
    months_dict = {'01':31,'02':{'leap':29,'normal':28},'03':31,'04':30,'05':31,
                  '06':30, '07':31, '08':31,'09':30,'10':31,'11':30,'12':31}
    
    if len(date) >= 8:
        if len(date) > 8: # handling exceptions for  2015 data
            date = date.split('.')[0]
            date = date.split(' ')[0].split('/')[-1]
        day_ext = int(date[-2:])
        month_ext = date[-4:-2]
        year = date[0:4]
        day_of_year = 0            
        for month in months_dict.keys():
            if month == month_ext:
                break
            if month == '02':
                if int(year)%4 == 0:
                    day_of_year += months_dict['02']['leap']
                else:
                    day_of_year += months_dict['02']['normal']
            else:
                day_of_year += months_dict[month]
        day_of_year += day_ext

        return day_of_year

def preprocess_csv(csv_year):
    if csv_year == '2015':
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
    dask_map = client.map(preprocess_csv, ['2013-14', '2015', '2016', '2017'])
    client.gather(dask_map)

#     parser = argparse.ArgumentParser()
#     parser.add_argument('--csv_year', type=str, help="year of csv ; possible values - 2013-14, 2015, 2016, 2017", required=True)
    
#     args = parser.parse_args()
    
#     assert args.csv_year in ['2013-14', '2015', '2016', '2017']
    
#     preprocess_csv(args.csv_year)