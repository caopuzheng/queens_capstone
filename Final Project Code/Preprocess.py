###This file is contating all the data preprocessing functions
import pandas as pd
import math
import mysql.connector as sql
from utility import *

####check the outlier change for the bond:
def check_outlier(x):
    if x > 2 or x < -2:
        return True
    else:
        return False

####Quickly Remerge the bond back to a pandas dataframe
def merge_left_right(data1,data2):
    temp_data = data1.append(data2)
    return temp_data

def merg_sort(list_data):
    list_length = len(list_data)
    if list_length == 1:
        output = list_data[0][2]
        return output
    else:
        mid_point = math.floor(list_length/2)
        #print(len(list_data[:mid_point]))
        left = list_data[mid_point:]
        right = list_data[:mid_point]
        return merge_left_right(merg_sort(left), merg_sort(right))

###merge all data togther(not in use anymore)
def merge_data(path,lists,initial_data):
    data = initial_data
    for file in lists:
        temp_data = pd.read_csv(path+file)
        print(len(temp_data))
        data = data.append(temp_data)
        print(len(data))
    return data


### Select data and calucate weekly data based on a given date range
def get_the_windows(bonds_list):
    print('start to transfer daily data into weekly data')
    bond_spread_list = []
    for bond in bonds_list:
        try:
            bond['KeyDate'] = pd.to_datetime(bond['KeyDate'])
            bond = bond.set_index('KeyDate')
            logic = {'GSpread'  : 'last',
                 'SecurityID':'first'}
            offset = pd.offsets.timedelta(days=-6)
            d = bond.resample('W', loffset=offset).apply(logic)
            d['G_change']=d.GSpread.pct_change()
            d.dropna(subset=['G_change'],inplace = True)
            d['outlier'] = d.G_change.apply(check_outlier)
            d.reset_index(inplace=True)
            if True not in d['outlier'].values:
                bond_spread_list.append((len(d.GSpread.values),bond.SecurityID.iloc[0],d))
        except:
            pass
    print('Done')
    return bond_spread_list

######Get all bonds' information in a given period and transfrom the pandas dataframe to a list of bonds.
def get_all_bonds_in_list(start_date,end_date):
    print('start to get data from {} to {}'.format(start_date,end_date))
    #####Create engine:
    db_connection = sql.connect(host='0.0.0.0', database='bond_db', user='root', password='password')
    ##security query
    security_query = "select * from security_info"
    ##read data
    security_data = pd.read_sql(security_query,con=db_connection)
    security_data = security_data[['SecurityID','Currency','IssueDate','MaturityDate']]
    ##price query
    price_query = "select * from bond_spread where  KeyDate between '{}' and '{}'".format(start_date,end_date)
    #####read bond_data from db:
    final_data = pd.read_sql(price_query,con=db_connection)
    final_data = final_data.merge(security_data, on=['SecurityID'], how='left')
    final_data.dropna(subset=["ZSpread"],inplace=True)
    final_data_1 = final_data[final_data.Currency == 'USD']
    final_data = final_data_1.groupby('SecurityID')
    bonds_list = [final_data.get_group(x) for x in final_data.groups]
    db_connection.close()
    return bonds_list

### Select data and calucate daily data based on a given date range
def get_the_daily_spread_windows(bonds_list):
    bond_spread_list = []
    for bond in bonds_list:
        try:
            bond['G_change_Percent']=bond.GSpread.pct_change()
            bond['G_change'] = bond.GSpread.diff()
            bond.dropna(subset=['G_change'],inplace = True)
            bond_spread_list.append((len(bond.GSpread.values),bond.SecurityID.iloc[0],bond))
        except:
            pass
    return bond_spread_list

### Select data and calucate Weekly data based on a given date range

def get_the_weekly_spread_windows(bonds_list):
    bond_spread_list = []
    for bond in bonds_list:
        try:
            bond['KeyDate'] = pd.to_datetime(bond['KeyDate'])
            bond = bond.set_index('KeyDate')
            logic = {'GSpread'  : 'last',
                    'ModifiedDuration_Plain':'last',
                    'YieldWorst':'last',
                    'AmtOutstanding':'last',
                 'SecurityID':'first'}
            offset = pd.offsets.timedelta(days=-6)
            d = bond.resample('W', loffset=offset).apply(logic)
            d['G_change_Percent']=d.GSpread.pct_change()
            d['G_change'] = d.GSpread.diff()
            d['ModifiedDuration_Plain_change']=d.ModifiedDuration_Plain.pct_change()
            d['YieldWorst_change']=d.YieldWorst.pct_change()
            d.dropna(subset=['G_change_Percent'],inplace = True)
            d.reset_index(inplace=True)
            bond_spread_list.append((len(d.GSpread.values),bond.SecurityID.iloc[0],d))
        except:
            pass
    return bond_spread_list

###calcuate the daily average GSpread per cluster
def Average_daily_Gspread_change_cluster(window,cluster_data):
    ####Grab Data###
    bond_list1 = get_all_bonds_in_list(window[0],window[1])
    ####G Change ###
    daily_spread_1 = get_the_weekly_spread_windows(bond_list1)
    new_data = merg_sort(daily_spread_1)
    new_data.dropna(inplace=True)
    data1 = new_data.merge(cluster_data[['SecurityID',window[1]]],on=['SecurityID'],how ='left')
    temp = data1.groupby(by=['KeyDate',window[1]],as_index=False).mean()
    data1['Group'] = data1[window[1]]
    data1.drop(columns=[window[1]],inplace=True)
    temp['Group'] = temp[window[1]]
    end = temp[['Group','KeyDate','G_change_Percent','YieldWorst_change','ModifiedDuration_Plain_change']]
    end = end.rename(columns={'G_change_Percent':'Cluster_G_change','YieldWorst_change':'Cluster_average_YieldWorst_change','ModifiedDuration_Plain_change':'Cluster_average_ModifiedDuration_Plain_change'})
    return end,data1

####transfer the data into lag(not in used)
def find_lag(X):
    a = X.T
    for index in a.index:
        columns_name = []
        for x in range(0,3):
            columns_name.append(index+'_lag_{}'.format(x+1))
        if index == 'YieldWorst':
            final = pd.DataFrame(a.loc[index].values.reshape(-1, 3),columns=columns_name)
        else:
            temp = pd.DataFrame(a.loc[index].values.reshape(-1, 3),columns=columns_name)
            final = pd.concat([final,temp],axis=1)
    return final

###Preprocess function for Regression Feature engineering
def get_the_daily_spread_windows(bonds_list):
    bond_spread_list = []
    for bond in bonds_list:
        try:
            bond['G_change']=bond.GSpread.pct_change()
            bond['ModifiedDuration_Plain_change']=bond.ModifiedDuration_Plain.pct_change()
            bond['YieldWorst_change']=bond.YieldWorst.pct_change()
            bond.dropna(subset=['G_change'],inplace = True)
            bond_spread_list.append((len(bond.GSpread.values),bond.SecurityID.iloc[0],bond))
        except:
            pass
    return bond_spread_list

### Use in getting cluster's information
def get_the_cluster_data(security_info, cluster_data, windows):
    print('start to get data for {}'.format(windows[1]))
    security_info = security_info[
        ['SecurityID', 'SecuritySector', 'AmountIssued', 'Coupon', 'Seniority', 'Term', 'RatingSP', 'Rating',
         'Type of Term']]
    a = security_info.merge(cluster_data[['SecurityID', windows[1]]], on=['SecurityID'], how='left')
    a.dropna(subset=[windows[1]], inplace=True)
    time_period = a.rename(columns={'2019-02-28': 'Group'})

    Group_Seniority = time_period.groupby(by=['Group', 'Seniority']).count()
    Group_sector = time_period.groupby(by=['Group', 'SecuritySector']).count()
    Group_Type_term = time_period.groupby(by=['Group', 'Type of Term']).count()
    Group_Rating = time_period.groupby(by=['Group', 'Rating']).count()

    Group = time_period.groupby(by=['Group']).count()
    Sector = time_period.groupby(by=['SecuritySector']).count()
    Type_of_Term = time_period.groupby(by=['Type of Term']).count()
    Seniority = time_period.groupby(by=['Seniority']).count()
    Rating = time_period.groupby(by=['Rating']).count()

    Sector_per = Sector / len(time_period)
    Type_of_Term_per = Type_of_Term / len(time_period)
    Seniority_per = Seniority / len(time_period)
    Rating_per = Rating / len(time_period)

    Group_sector_per = Group_sector / Group
    Group_Seniority_per = Group_Seniority / Group
    Group_type_term_per = Group_Type_term / Group
    Group_rating_per = Group_Rating / Group

    Group_Sector_indicator = (Group_sector_per - Sector_per) / (1 - Sector_per)
    Group_Seniority_indicator = (Group_Seniority_per - Seniority_per) / (1 - Seniority_per)
    Group_type_term_indicator = (Group_type_term_per - Type_of_Term_per) / (1 - Type_of_Term_per)
    Group_rating_indicator = (Group_rating_per - Rating_per) / (1 - Rating_per)

    for i in range(0, 125):
        if i == 0:
            f = Group_Seniority_indicator.loc['Cluster {}'.format(i)].T.head(1).reset_index().iloc[:, 1:]
            f['Group'] = 'Cluster {}'.format(i)
        else:
            temp = Group_Seniority_indicator.loc['Cluster {}'.format(i)].T.head(1).reset_index().iloc[:, 1:]
            temp['Group'] = 'Cluster {}'.format(i)
            f = f.merge(temp, how='outer').fillna(0)
    for i in range(0, 125):
        if i == 0:
            f1 = Group_Sector_indicator.loc['Cluster {}'.format(i)].T.head(1).reset_index().iloc[:, 1:]
            f1['Group'] = 'Cluster {}'.format(i)
        else:
            temp1 = Group_Sector_indicator.loc['Cluster {}'.format(i)].T.head(1).reset_index().iloc[:, 1:]
            temp1['Group'] = 'Cluster {}'.format(i)
            f1 = f1.merge(temp1, how='outer').fillna(0)
    for i in range(0, 125):
        if i == 0:
            f2 = Group_type_term_indicator.loc['Cluster {}'.format(i)].T.head(1).reset_index().iloc[:, 1:]
            f2['Group'] = 'Cluster {}'.format(i)
        else:
            temp2 = Group_type_term_indicator.loc['Cluster {}'.format(i)].T.head(1).reset_index().iloc[:, 1:]
            temp2['Group'] = 'Cluster {}'.format(i)
            f2 = f2.merge(temp2, how='outer').fillna(0)
    for i in range(0, 125):
        if i == 0:
            f3 = Group_rating_indicator.loc['Cluster {}'.format(i)].T.head(1).reset_index().iloc[:, 1:]
            f3['Group'] = 'Cluster {}'.format(i)
        else:
            temp3 = Group_rating_indicator.loc['Cluster {}'.format(i)].T.head(1).reset_index().iloc[:, 1:]
            temp3['Group'] = 'Cluster {}'.format(i)
            f3 = f3.merge(temp3, how='outer').fillna(0)
    f = f.merge(f1, on='Group', how='left')
    f = f.merge(f2, on='Group', how='left')
    f = f.merge(f3, on='Group', how='left')
    b = time_period.groupby(by=['Group'], as_index=False).mean()
    b.drop(columns=['SecurityID'], inplace=True)
    b = b.rename(columns={'AmountIssued': 'Cluster_average_AmountIssued', 'Coupon': 'Cluster_average_Coupon',
                          'Term': 'Cluster_average_Term', 'RatingSP': 'Cluster_average_Rating'})
    window_cluster_data = b.merge(f, on='Group', how='left')
    return window_cluster_data

###Generate the cluster feature
def feature_engineer_cluster_data(security_info,cluster_data,windows):
    print('start to get data for {}'.format(windows[1]))
    security_info = security_info[['SecurityID','SecuritySector','AmountIssued','Coupon','Seniority','Term','Type of Term','RatingSP','Rating']]
    a = security_info.merge(cluster_data[['SecurityID',windows[1]]],on=['SecurityID'],how='left')
    a.dropna(subset=[windows[1]],inplace=True)
    a = a.rename(columns={windows[1]:'Group'})
    c = a.groupby(by=['Group','Seniority']).count()
    s= a.groupby(by=['Group','SecuritySector']).count()
    r = a.groupby(by=['Group','Rating']).count()
    t = a.groupby(by=['Group','Type of Term']).count()
    d = a.groupby(by=['Group']).count()
    e = c/d
    e1 = s/d
    e2 = r/d
    e3 = t/d
    list1 = [e,e1,e2,e3]
    f = combine_data(list1,125)
    b = a.groupby(by=['Group'],as_index=False).mean()
    b.drop(columns=['SecurityID'],inplace=True)
    b = b.rename(columns={'AmountIssued':'Cluster_average_AmountIssued','Coupon':'Cluster_average_Coupon','Term':'Cluster_average_Term','RatingSP':'Cluster_average_Rating'})
    window_cluster_data = b.merge(f,on='Group',how = 'left')
    return window_cluster_data