import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns
sns.set()
import argparse
import yaml
from datetime import datetime
import os
from multiprocessing import Pool

config = yaml.load(open("config.yml", "r"), yaml.SafeLoader)

def get_plot(csv_year):
    df_kmeans_pca = pd.read_csv(config['KMeans']['2013-14'])
    x_axis = df_kmeans_pca['Component 2']
    y_axis = df_kmeans_pca['Component 1']
    plt.figure(figsize=(12,9))
    sns.scatterplot(x_axis,y_axis,hue=df_kmeans_pca['Segment'],palette=['r','g','b'])
    plt.title('Clusters by PCA Components')

    if not(os.path.isdir(config['artifacts']['path'])):
        os.makedirs(config['artifacts']['path'])

    plt.savefig(f"{config['artifacts']['path']}/scatter_{str(datetime.now()).replace(' ','_')}_{csv_year}.png")

if __name__ == "__main__":
    with Pool(6) as p:
        p.map(get_plot,['2013-14',
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
