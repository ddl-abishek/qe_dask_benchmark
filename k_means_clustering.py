import dask.dataframe as dd
import dask.array as da
from dask.distributed import Client, performance_report
from dask_ml.cluster import KMeans
from dask_ml.preprocessing import StandardScaler
from dask_ml.decomposition import PCA
import numpy as np
from collections import defaultdict
from matplotlib import pyplot as plt
import seaborn as sns
sns.set()
import argparse
import yaml
from datetime import datetime
import os

config = yaml.load(open("config.yml", "r"), yaml.SafeLoader)

def get_kmeans_pca(csv_year):
    clean_data = dd.read_csv(config['clean_data'][csv_year], 
                             usecols=['Vehicle Expiration Date', 'Violation Precinct', 'Issuer Precinct', 'Vehicle Year'], 
                             dtype={'Vehicle Expiration Date' : float,
                                    'Violation Precinct' : float,
                                    'Issuer Precinct' : float,
                                    'Vehicle Year' : float}).dropna()
    
    # normalizing the dataset
    std_clean_data = StandardScaler().fit_transform(clean_data)
    
    # applying principal component analysis 
    pca = PCA(n_components = config['PCA']['n_components'],svd_solver='auto')
    pca.fit(std_clean_data.to_dask_array(lengths=True))
    # calculating the resulting components scores for the elements in our data set
    scores_pca = pca.transform(clean_data.to_dask_array(lengths=True))
    
    # clustering via k means
    kmeans_pca = KMeans(n_clusters = config['KMeans']['n_clusters'], 
                        init = config['KMeans']['init'], 
                        random_state = config['KMeans']['random_state'])
    kmeans_pca.fit(scores_pca)
    
    scores_pca = dd.from_array(scores_pca,columns=['Component 1','Component 2','Component 3'])
    clean_data = clean_data.repartition(npartitions=3)
    scores_pca = scores_pca.repartition(npartitions=3)
    df_kmeans_pca = dd.concat([clean_data.reset_index(drop=True),scores_pca.reset_index(drop=True)],axis=1)

    # the last column we add contains the pca k-means clutering labels
    df_kmeans_pca['Segment K-means PCA'] = kmeans_pca.labels_
    df_kmeans_pca['Segment'] = df_kmeans_pca['Segment K-means PCA'].map({0:'first',1:'second',2:'third'})
    df_kmeans_pca = df_kmeans_pca.drop(columns='Segment K-means PCA')
    
    if not(os.path.isdir(config['KMeans']['dir'])):
        os.makedirs(config['KMeans']['dir'])

    df_kmeans_pca.compute().to_csv(config['KMeans'][csv_year], index=False)

if __name__ == "__main__":
    service_port = os.environ['DASK_SCHEDULER_SERVICE_PORT']
    service_host = os.environ['DASK_SCHEDULER_SERVICE_HOST']

    client = Client(address=f'{service_host}:{service_port}', direct_to_workers=True)
    client.wait_for_workers(n_workers=2)
    client.restart()
    
    with performance_report(filename=f"{config['artifacts']['path']}/dask-report_k_means_clustering_{str(datetime.now())}.html"):
        dask_map = client.map(get_kmeans_pca,['2013-14','2015','2016','2017'])
        client.gather(dask_map)
    client.close()
