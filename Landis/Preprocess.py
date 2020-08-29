###This file is contating all the data preprocessing functions
import pandas as pd
import math
import mysql.connector as sql

####check the outlier change for the bond:
def check_outlier(x):
    if x > 2 or x < -2:
        return True
    else:
        return False

####Merge sort
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

###merge all data togther
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
            bond['G_change']=bond.GSpread.pct_change()
            bond.dropna(subset=['G_change'],inplace = True)
            bond_spread_list.append((len(bond.GSpread.values),bond.SecurityID.iloc[0],bond))
        except:
            pass
    return bond_spread_list
###calcuate the daily average GSpread per cluster
def Average_daily_Gspread_change_cluster(window,cluster_data):
    bond_list1 = get_all_bonds_in_list(window[0],window[1])
    daily_spread_1 = get_the_daily_spread_windows(bond_list1)
    new_data = merg_sort(daily_spread_1)
    new_data.dropna(inplace=True)
    data1 = new_data.merge(cluster_data[['SecurityID',window[1]]],on=['SecurityID'],how ='left')
    temp = data1.groupby(by=['KeyDate',window[1]],as_index=False).mean()
    data1['Group'] = data1[window[1]]
    temp['Group'] = temp[window[1]]
    end = temp[['Group','KeyDate','G_change']]
    return end,data1