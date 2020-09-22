## Using Hierarchical method to cluster the data
import pandas as pd
from Preprocess import get_all_bonds_in_list, get_the_windows, merg_sort
from utility import grab_clusters, create_cluster_list, transfer_to_list_of_pd, process_rating_data,collect_cluster_info
from sklearn.cluster import AgglomerativeClustering
import pickle
import mysql.connector as sql
from feature_extraction import tsfresh_extract
from windows import silding_windows

silding_windows = silding_windows

####read security data:
db_connection = sql.connect(host='0.0.0.0', database='bond_db', user='root', password='password')
security_query = "select * from security_info"
security_data = pd.read_sql(security_query, con=db_connection)


################################
### Hierarchical APPROACH ######
################################

def Hierarchical(data_scaled, number_of_cluster, date,fdata):
	print('start to do Hierarchical Model with {} cluster'.format(number_of_cluster))
	Hierarchical = AgglomerativeClustering(n_clusters=number_of_cluster, affinity='euclidean', linkage='average')
	cluster2_array = Hierarchical.fit_predict(data_scaled)
	clusters2 = create_cluster_list(cluster2_array)
	clusters_list2 = grab_clusters(clusters2, fdata, security_data, rating_data)
	clusters_data_list2 = transfer_to_list_of_pd(clusters_list2)
	pickle.dump(Hierarchical, open("Bond_Hierarchical_Clustering_{}Groups_{}.pkl".format(number_of_cluster, date), "wb"))
	print('Saved Hierarchical, Grabed Cluster information','Done')
	return clusters_data_list2


####Create a dict with cluster_list through year
clusters_dict = {}
for i in silding_windows:
	start_date = i[0]
	end_date = i[1]
	#####read rating data:
	rating_query = "select * from rating where KeyDate between '{}' and '{}'".format(start_date, end_date)
	rating_data = pd.read_sql(rating_query, con=db_connection)
	rating_data = process_rating_data(rating_data)
	#####create the bonds list
	bonds_list = get_all_bonds_in_list(start_date, end_date)
	####transfer the daily data into weekly data
	bond_spread_list = get_the_windows(bonds_list)
	new_data = merg_sort(bond_spread_list)
	new_data.dropna(inplace=True)
	data_scaled,fdata = tsfresh_extract(new_data)
	cluster = Hierarchical(data_scaled, 125, i[1],fdata)
	if i[1] not in clusters_dict.keys():
		clusters_dict[i[1]] = cluster
##Close the db connection
db_connection.close()

for i in range(0, len(clusters_dict.keys())):
	end_date = list(clusters_dict.keys())[i]
	temp_list = clusters_dict[end_date]
	if i == 0:
		final_cluster = collect_cluster_info(temp_list, end_date)
	else:
		temp_data = collect_cluster_info(temp_list, end_date)
		final_cluster = final_cluster.merge(temp_data,on=['SecurityID'],how='outer')

###Save Your Final Hierarchical Cluster
final_cluster.to_csv('Data/Cluster_group_Hierarchical.csv')
print('Done')
