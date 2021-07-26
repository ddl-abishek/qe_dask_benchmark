import dask.dataframe as dd
import dask.array as da
from dask.distributed import Client


# from sklearn.cluster import KMeans
from dask_ml.cluster import KMeans

# from sklearn.preprocessing import StandardScaler
from dask_ml.preprocessing import StandardScaler

# from sklearn.decomposition import PCA
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
    clean_data = clean_data.repartition(npartitions=5)
    scores_pca = scores_pca.repartition(npartitions=5)
    df_kmeans_pca = dd.concat([clean_data.reset_index(drop=True),scores_pca.reset_index(drop=True)],axis=1)

    # the last column we add contains the pca k-means clutering labels
    df_kmeans_pca['Segment K-means PCA'] = kmeans_pca.labels_
    df_kmeans_pca['Segment'] = df_kmeans_pca['Segment K-means PCA'].map({0:'first',1:'second',2:'third'})
    df_kmeans_pca = df_kmeans_pca.drop(columns='Segment K-means PCA')

    x_axis = df_kmeans_pca['Component 2']
    y_axis = df_kmeans_pca['Component 1']
    plt.figure(figsize=(12,9))
    sns.scatterplot(x_axis,y_axis,hue=df_kmeans_pca['Segment'],palette=['r','g','b'])
    plt.title('Clusters by PCA Components')
    
    if not(os.path.isdir(config['artifacts']['path'])):
        os.makedirs(config['artifacts']['path'])

    plt.savefig(f"{config['artifacts']['path']}/scatter_{str(datetime.now()).replace(' ','_')}_{csv_year}.png")

if __name__ == "__main__":
    service_port = os.environ['DASK_SCHEDULER_SERVICE_PORT']
    service_host = os.environ['DASK_SCHEDULER_SERVICE_HOST']

    client = Client(address=f'{service_host}:{service_port}', direct_to_workers=True)
    dask_map = client.map(get_kmeans_pca, ['2013-14', '2015', '2016', '2017'])
    client.gather(dask_map)
    
#     parser = argparse.ArgumentParser()
#     parser.add_argument('--csv_year', type=str,help="year of .csv ; possible values - 2013-14, 2015, 2016, 2017",required=True)
    
#     args = parser.parse_args()
    
#     assert args.csv_year in ['2013-14', '2015', '2016', '2017']
    
#     df_kmeans_pca = get_kmeans_pca(args.csv_year)
#     plot(df_kmeans_pca, args.csv_year)