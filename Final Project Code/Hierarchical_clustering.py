## Using Hierarchical method to cluster the data
import pandas as pd
from Preprocess import *
from utilitie import *
from distance_metric import *
from Deminison_Reduction import *
import math

start_date ='2018-12-31'
time_list = start_date.split('-')
year = int(time_list[0])
month = int(time_list[1])
date = int(time_list[2])
start_datetime = datetime.datetime(year, month, date)
#####windows defult 62
bond_spread_list,test_cluster_1_with_spread = process_data(start_datetime)
###################
###security data###
###################
security_data = pd.read_csv("C:/Users/y437l/OneDrive/MMAI/Capstone/Data/SecurityData - Copy-LAPTOP-OAEJRPE8.csv")

from pyclustering.cluster.hsyncnet import hsyncnet
# read list of points for cluster analysis
sample = test_cluster_1_with_spread
# create network for allocation three clusters using CCORE (C++ implementation)
network = hsyncnet(sample, 3, ccore = True);
# run cluster analysis and output dynamic of the network
(time, dynamic) = network.process(0.995, collect_dynamic = True)
# get allocated clustersE
clusters = network.get_clusters()
print(len(clusters))
clusters_list = grab_clusters(clusters,bond_spread_list,security_data)

