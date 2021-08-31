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
    df_kmeans_pca = pd.read_csv(config['KMeans'][csv_year])
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
        p.map(get_plot, ['2013-14','2015','2016','2017'])
