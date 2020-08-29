import pandas as pd
import mysql.connector as sql
import math
from Preprocess import Average_daily_Gspread_change_cluster
from utility import dump_data
silding_windows = [['2018-12-31','2019-02-28'],['2019-03-01','2019-04-30'],
                   ['2019-05-01','2019-06-30'],['2019-07-01','2019-08-31'],
                   ['2019-09-01','2019-10-31'],['2019-11-01','2019-12-31']]

cluster_data = pd.read_csv('Cluster_group.csv')
cluster_data.drop(columns=['Unnamed: 0'],inplace=True)

for i in silding_windows:
    temp_data,temp_d = Average_daily_Gspread_change_cluster(i,cluster_data)
    dump_data(temp_data,'bond_db','Average_cluster_change')