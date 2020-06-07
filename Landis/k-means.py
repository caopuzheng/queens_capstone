## Using K-means method to cluster the data
from Preprocess import *
from utilitie import *
from distance_metric import *
import pandas as pd
import datetime
import time

start_date ='2018-12-31'
time_list = start_date.split('-')
year = int(time_list[0])
month = int(time_list[1])
date = int(time_list[2])
start_datetime = datetime.datetime(year, month, date)
###find the end date:
end_datetime = start_datetime + datetime.timedelta(days=90)
end_date = str(end_datetime)

#####windows defult 62
bond_spread_list,test_cluster_1_with_spread_change = process_data(start_date)

###################
###security data###
###################
security_data = pd.read_csv("C:/Users/y437l/OneDrive/MMAI/Capstone/Data/SecurityData - Copy-LAPTOP-OAEJRPE8.csv")
###################
### Rating Data####
###################
rating_data = process_rating_data(pd.read_csv("C:/Users/y437l/OneDrive/MMAI/Capstone/Data/Rating.csv"))

##########################
### K Mean APPROACH ######
###########################
from pyclustering.cluster.elbow import elbow
from pyclustering.cluster.kmeans import kmeans
from pyclustering.cluster.center_initializer import kmeans_plusplus_initializer
from pyclustering.utils.metric import type_metric, distance_metric
# read sample 'Simple3' from file (sample contains four clusters)
market_data = test_cluster_1_with_spread_change
user_function = lambda series1, series2: DTWDistance(series1, series2)
#metric = distance_metric(type_metric.USER_DEFINED, func =user_function)
metric = distance_metric(type_metric.EUCLIDEAN)
# create instance of Elbow method using K value from 1 to 10.
kmin, kmax = 1, 5
elbow_instance = elbow(market_data, kmin, kmax,metric=metric)
# process input data and obtain results of analysis
elbow_instance.process()
amount_clusters = elbow_instance.get_amount()   # most probable amount of clusters
wce = elbow_instance.get_wce()                  # total within-cluster errors for each K
# perform cluster analysis using K-Means algorith
centers = kmeans_plusplus_initializer(market_data, amount_clusters).initialize()
kmeans_instance = kmeans(market_data, centers)
kmeans_instance.process()
# obtain clustering results and visualize them
clusters = kmeans_instance.get_clusters()
centers = kmeans_instance.get_centers()
clusters_list = grab_clusters(clusters,bond_spread_list,security_data,rating_data)
clusters_data_list = transfer_to_list_of_pd(clusters_list)