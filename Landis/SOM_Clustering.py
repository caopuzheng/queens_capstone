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
#######################
### SOM APPROACH ######
#######################
import random
from pyclustering.utils import read_sample
from pyclustering.nnet.som import som, type_conn, type_init, som_parameters
from pyclustering.samples.definitions import FCPS_SAMPLES
# read sample 'Lsun' from file
sample = test_cluster_1_with_spread_change
# create SOM parameters
parameters = som_parameters()
# create self-organized feature map with size 7x7
rows = 100  # five rows
cols = 100  # five columns
structure = type_conn.grid_four;  # each neuron has max. four neighbors.
network = som(rows, cols, structure, parameters)
# train network on 'Lsun' sample during 100 epouchs.
network.train(sample, 100)
# simulate trained network using randomly modified point from input dataset.
index_point = random.randint(0, len(sample) - 1)
point = sample[index_point]  # obtain randomly point from data
point[0] += random.random() * 0.2  # change randomly X-coordinate
point[1] += random.random() * 0.2  # change randomly Y-coordinate
index_winner = network.simulate(point)
# check what are objects from input data are much close to randomly modified.
index_similar_objects = network.capture_objects[index_winner]
# neuron contains information of encoded objects
print("Point '%s' is similar to objects with indexes '%s'." % (str(point), str(index_similar_objects)))
print("Coordinates of similar objects:")
for index in index_similar_objects: print("\tPoint:", sample[index])
# result visualization:
# show distance matrix (U-matrix).
network.show_distance_matrix()
# show density matrix (P-matrix).
network.show_density_matrix()
# show winner matrix.
network.show_winner_matrix()
# show self-organized map.
network.show_network()