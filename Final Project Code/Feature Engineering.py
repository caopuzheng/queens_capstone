###  This feature engineer folder for regression
import pandas as pd
import mysql.connector as sql
import math
from utility import *
from Preprocess import *
from windows import silding_windows

silding_windows = silding_windows
### Read Cluster groups here
cluster_data = pd.read_csv('Data/Cluster_group_Hierarchical.csv')

### Read the rating data from the database
db_connection = sql.connect(host='0.0.0.0', database='bond_db', user='root', password='password')
rating_query = "select * from rating"
rating_data = pd.read_sql(rating_query,con=db_connection)

### Read the Security information from the database
db_connection = sql.connect(host='0.0.0.0', database='bond_db', user='root', password='password')
security_query = "select * from security_info"
security_data = pd.read_sql(security_query,con=db_connection)

###Get the average Rating for the Security:
rating_data = rating_data.groupby(by=['SecurityID'],as_index=False).mean()

###Get The Bond's Term for Security:
security_data['Term'] = security_data.apply(lambda x:term(x.MaturityDate,x.IssueDate),axis=1)

###Get The Type of Term, if the MaturityDate is missing set the Term to 100
security_data['Term'] = security_data['Term'].fillna(100)
security_data['Type of Term'] = security_data['Term'].apply(Type_term)

###Merge the rating data with the security data
security_data = security_data.merge(rating_data,on =['SecurityID'],how='left')

###Get the Credit Rating
security_data['Rating'] = security_data['RatingSP'].apply(assign_rating)

###Scan through window list and return data sets:
### Final_change contains Cluster_G_change	Cluster_average_YieldWorst_change	Cluster_average_ModifiedDuration_Plain_change
### Final contains other cluster data
for i in range(0,len(silding_windows)):
    if i == 0:
        ###chaneg daily to weekly if you want to have weekly spread change
        final_change,final = Average_Gspread_abs_change_cluster(silding_windows[i],cluster_data,'daily')
        temp_cluster = feature_engineer_cluster_data(security_data,cluster_data,silding_windows[i])
        final = final.merge(temp_cluster, on='Group',how='left')
    else:
        temp,temp_data = Average_Gspread_abs_change_cluster(silding_windows[i],cluster_data,'daily')
        temp_cluster = feature_engineer_cluster_data(security_data,cluster_data,silding_windows[i])
        temp_data = temp_data.merge(temp_cluster, on='Group',how='left')
        final_change = final_change.append(temp,ignore_index=True)
        final = final.append(temp_data,ignore_index=True)

### Select the columns
final = final[['SecurityID','KeyDate','YieldWorst','ModifiedDuration_Plain','AmtOutstanding','G_change','YieldWorst_change',
                'ModifiedDuration_Plain_change','Group','Cluster_average_AmountIssued',
                'Cluster_average_Coupon', 'Cluster_average_Term',
                'Cluster_average_Rating', 'Jr Subordinated Unsecured', 'Secured',
                'Sr Unsecured', 'Subordinated Unsecured', '1st Lien Secured',
                '2nd Lien Secured', '3rd Lien Secured', '1st lien', 'Asset Backed',
                'Auto', 'Basic Materials','Communications', 'Consumers', 'Energy', 'Financials', 'Health Care',
                'Industrials', 'Real Estate', 'Technology', 'High grade', 'Junk',
                'Lower medium grade', 'UN', 'Upper medium grade', 'Prime', 'Long Term',
                'Mid Term', 'Short Term']]

##Rename the existing columns
final = final.rename(columns={ 'Jr Subordinated Unsecured':'Cluster Perecentage of Jr Subordinated Unsecured',
                               'Secured':'Cluster Perecentage of Secured',
                               'Sr Unsecured':'Cluster Perecentage of Sr Unsecured',
                               'Subordinated Unsecured':'Cluster Perecentage of Subordinated Unsecured',
                               '1st Lien Secured':'Cluster Perecentage of 1st Lien Secured',
                               '2nd Lien Secured':'Cluster Perecentage of 2nd Lien Secured',
                               '3rd Lien Secured':'Cluster Perecentage of 3rd Lien Secured',
                               '1st lien':'Cluster Perecentage of 1st lien',
                               'Asset Backed':'Cluster Perecentage of Asset Backed',
                               'Auto':'Cluster Perecentage of Auto',
                               'Basic Materials':'Cluster Perecentage of Basic Materials',
                               'Communications':'Cluster Perecentage of Communications',
                               'Consumers':'Cluster Perecentage of Consumers',
                               'Energy':'Cluster Perecentage of Energy',
                               'Financials':'Cluster Perecentage of Financials',
                               'Health Care':'Cluster Perecentage of Health Care',
                               'Industrials':'Cluster Perecentage of Industrials',
                               'Real Estate':'Cluster Perecentage of Real Estate',
                               'Technology':'Cluster Perecentage of Technology',
                               'High grade':'Cluster Perecentage of High grade',
                               'Junk':'Cluster Perecentage of Junk',
                               'Lower medium grade':'Cluster Perecentage of Lower medium grade',
                               'UN':'Cluster Perecentage of UN',
                               'Upper medium grade':'Cluster Perecentage of Upper medium grade',
                               'Prime':'Cluster Perecentage of Prime',
                               'Long Term':'Cluster Perecentage of Long Term',
                               'Mid Term':'Cluster Perecentage of Mid Term',
                               'Short Term':'Cluster Perecentage of Short Term',
                               "G_change": "Target_G_change"})
### Merge the final with the changing data
final = final.merge(final_change,on=['Group','KeyDate'],how = 'left')
### fill na with zero
final = final.fillna(0)

## Grab the Market data
Market_query = "select * from market_data_weekly"
Market_data = pd.read_sql(Market_query,con=db_connection)
Market_data = Market_data.fillna(0)

##Select the Market data columns uncommen it if you want to generate a weekly change:
#Market_data = Market_data[['KeyDate','skew_change_Weekly','swap_change_Weekly','vix_change_Weekly','Close_change_Weekly','slope_change_Weekly']]

##Merge Market data with the final data
final_regression_data = final.merge(Market_data,on=['KeyDate'],how='left')

####Change your saving file's name here:
final_regression_data.to_csv('Data/Security_and_market_movement_unscaled_cluster_125_H.csv')

########################
### DUMMY VARIABLE #####
########################

##get dummy variable of Seniority
df = pd.concat([security_data['SecurityID'],pd.get_dummies(security_data.Seniority)],axis=1)
##get dummy variable of SecuritySector
df = pd.concat([df,pd.get_dummies(security_data.SecuritySector)],axis=1)
##get dummy variable of CountryRisk
df = pd.concat([df,pd.get_dummies(security_data.CountryRisk)],axis=1)
##get dummy variable of CouponType
df = pd.concat([df,pd.get_dummies(security_data.CouponType)],axis=1)
### Merge the other information from security
df_unsacled = pd.concat([df,security_data[['MinIncrement','MinPiece','Term','RatingSP']]],axis=1)

##Change your saving file's name here
df_unsacled.to_csv('Data/Security_info_post_dummy_H.csv')