import pandas as pd
import mysql.connector as sql
import math
from Preprocess import Average_daily_Gspread_change_cluster,get_all_bonds_in_list, get_the_windows, merg_sort,get_the_cluster_data
from utility import dump_data
import pandas as pd

cluster_data = pd.read_csv('Cluster_group_125.csv')
cluster_data.drop(columns=['Unnamed: 0'],inplace=True)

db_connection = sql.connect(host='0.0.0.0', database='bond_db', user='root', password='password')
security_query = "select * from security_info"
security_data = pd.read_sql(security_query,con=db_connection)

##Market query
Market_query = "select * from market_data_weekly"
Market_data = pd.read_sql(Market_query,con=db_connection)
Market_data = Market_data.fillna(0)

silding_windows = [['2018-12-31','2019-02-28'],['2019-03-01','2019-04-30'],
                   ['2019-05-01','2019-06-30'],['2019-07-01','2019-08-31'],
                   ['2019-09-01','2019-10-31'],['2019-11-01','2019-12-31'],
                   ['2020-01-01','2020-02-29']]


for i in range(0,len(silding_windows)):
    if i == 0:
        final_change,final = Average_daily_Gspread_change_cluster(silding_windows[i],cluster_data)
        temp_cluster = get_the_cluster_data(security_data,cluster_data,silding_windows[i],125)
        final = final.merge(temp_cluster, on='Group',how='left')
    else:
        temp,temp_data = Average_daily_Gspread_change_cluster(silding_windows[i],cluster_data)
        temp_cluster = get_the_cluster_data(security_data,cluster_data,silding_windows[i])
        temp_data = temp_data.merge(temp_cluster, on='Group',how='left')
        final_change = final_change.append(temp,ignore_index=True)
        final = final.append(temp_data,ignore_index=True)

####################################
#Select information form the final #
###################################
final = final[['SecurityID','KeyDate','YieldWorst','ModifiedDuration_Plain','AmtOutstanding','G_change','YieldWorst_change',
                'ModifiedDuration_Plain_change','Group','Cluster_average_AmountIssued',
                'Cluster_average_Coupon', 'Cluster_average_Term',
                'Cluster_average_Rating', 'Jr Subordinated Unsecured', 'Secured',
                'Sr Unsecured', 'Subordinated Unsecured', '1st Lien Secured',
                '2nd Lien Secured', '3rd Lien Secured', '1st lien', 'Asset Backed']]
### change the data name
final = final.rename(columns={"G_change": "Target_G_change"})
### merge daily_change_data with security_information
final = final.merge(final_change,on=['Group','KeyDate'],how = 'left')
final.dropna(subset=['Group'],inplace=True)

#
final_change_data = final.merge(Market_data,on=['KeyDate'],how='left')
final_change_data.to_csv('Security_and_market_movement_unscaled_cluster125.csv')