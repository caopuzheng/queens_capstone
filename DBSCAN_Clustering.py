from Preprocess import *
from utilitie import *
from distance_metric import *
from Deminison_Reduction import *
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
### DBSCAN APPROACH ######
###########################
from pyclustering.cluster.dbscan import dbscan
from pyclustering.utils.metric import type_metric, distance_metric
# Load list of points for cluster analysis.
sample = test_cluster_1_with_spread_change
####DTW
user_function = lambda series1, series2: DTWDistance(series1, series2)
#####Distance correlation
###user_function = lambda series1, series2: DCDistance(series1, series2)
metric = distance_metric(type_metric.USER_DEFINED, func =user_function)
# Create DBSCAN algorithm.
dbscan_instance = dbscan(sample,0.3, 1,metric=metric)
# Start processing by DBSCAN.
dbscan_instance.process()
# Obtain results of clustering.
clusters = dbscan_instance.get_clusters()
print(len(clusters))
noise = dbscan_instance.get_noise()
print(len(noise))

clusters_list = grab_clusters(clusters,bond_spread_list,security_data,rating_data)
clusters_data_list = transfer_to_list_of_pd(clusters_list)