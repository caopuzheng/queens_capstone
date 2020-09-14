import pandas as pd
import mysql.connector as sql
import math
from utility import *
from Preprocess import *
from windows import silding_windows

silding_windows = silding_windows
## Security
db_connection = sql.connect(host='0.0.0.0', database='bond_db', user='root', password='password')
security_query = "select * from security_info"
security_data = pd.read_sql(security_query,con=db_connection)

#Rating
db_connection = sql.connect(host='0.0.0.0', database='bond_db', user='root', password='password')
rating_query = "select * from rating"
rating_data = pd.read_sql(rating_query,con=db_connection)

###Change your cluster group here
cluster_data = pd.read_csv('Data/Cluster_group_125.csv')
cluster_data.drop(columns=['Unnamed: 0'],inplace=True)

## Get the rating for security
rating_data = rating_data.groupby(by=['SecurityID'],as_index=False).mean()
###Get the Term for security
security_data['Term'] = security_data.apply(lambda x:term(x.MaturityDate,x.IssueDate),axis=1)
security_data['Term'] = security_data['Term'].fillna(100)
security_data['Type of Term'] = security_data['Term'].apply(Type_term)
#Get the Credit rating for security
security_data = security_data.merge(rating_data,on =['SecurityID'],how='left')
security_data['Rating'] = security_data['RatingSP'].apply(assign_rating)

###get the cluster composition
cluster_info = get_the_cluster_data(security_data,cluster_data,silding_windows[0])

###save your cluster compostion raw data
cluster_info.to_csv('Data/test2.csv')