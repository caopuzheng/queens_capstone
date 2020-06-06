import pandas as pd
from Preprocess import *
from utilitie import *
from distance_metric import *
from Deminison_Reduction import *
import math

#####windows defult 62
bond_spread_list,test_cluster_1_with_spread = process_data()

#######################
### SOM APPROACH ######
#######################
from pyclustering.cluster.syncsom import syncsom
# read sample for clustering
sample = test_cluster_1_with_spread
# create oscillatory network for cluster analysis where the first layer has
# size 10x10 and connectivity radius for objects 1.0.
network = syncsom(sample, 100, 100, 0.5);
# simulate network (perform cluster analysis) and collect output dynamic
(dyn_time, dyn_phase) = network.process(True, 0.998);
# obtain encoded clusters
encoded_clusters = network.get_som_clusters();
# obtain real clusters
clusters = network.get_clusters();
# show the first layer of the network
network.show_som_layer();
# show the second layer of the network
network.show_sync_layer();