###This file is contating all the data preprocessing functions
from utilitie import *
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import time
import math

##define a merge sort algrithm to handle the bond data:
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

# Merge the file all togther.
## List of files -> pd dataframe.
def merge_data(lists,initial_data):
    data = initial_data
    for file in lists:
        path = 'C:\\Users\\y437l\\OneDrive\\MMAI\\Capstone\\Data\\{}'.format(file)
        temp_data = pd.read_csv(path)
        print(len(temp_data))
        data = data.append(temp_data)
        print(len(data))
    return data

## Select the data with excat trading days.
def select_data(num,listoftuble):
    temp_list = []
    for i in listoftuble:
        if i[0] == num:
            temp_list.append(i)
    return temp_list

## Select the data with only spread percentage change.
def select_spread_data(listoftuble):
    temp_list = []
    for i in listoftuble:
        temp_list.append(i[2])
    return temp_list

## Apply fill the empty Gspeard in the database.
def apply_fill(data_list,index):
    for i in data_list:
        try:
            fill_the_GSpeard(i)
        except:
            index.append(i)
    return index,data_list

## Assign the rating key according to the bond's average rating score
def process_rating_data(data):
    #data = data[data['KeyDate']<=end_date]
    rating = data.groupby('SecurityID',as_index=False).mean()
    rating['RatingSP'] = rating['RatingSP'].apply(lambda x:assign_rating(x))
    return rating

####Data perprocess pipline
def process_data(start_date,windows=61):
    start_time = time.time()
    security_data = pd.read_csv("C:/Users/y437l/OneDrive/MMAI/Capstone/Data/SecurityData - Copy-LAPTOP-OAEJRPE8.csv")
    security_data =security_data[['SecurityID','Currency','IssueDate','MaturityDate']]
    ### merge spread data togther
    sc = MinMaxScaler()
    data = pd.read_csv("C:/Users/y437l/OneDrive/MMAI/Capstone/Data/1.csv")
    file_lists = ['14426.csv','24001.csv','36128.csv','48087.csv','55086.csv']
    final_data = merge_data(file_lists,data)
    final_data.dropna(subset=["ZSpread"], inplace=True)
    #final_data = data
    ###left join the currency data into the spread data
    final_data = final_data.merge(security_data, on=['SecurityID'], how='left')
    ###select currency as USD
    final_data_1 = final_data[final_data.Currency == 'USD']
    final_data = final_data_1.groupby('SecurityID')
    ###create a list of bond with data
    bonds_list = [final_data.get_group(x) for x in final_data.groups]

    #########################################################
    print("--- %s seconds ---" % (time.time() - start_time))
    ##########################################################
    ######apply fill missing value function to bonds_list:
    ### Extract the problematic index
    problemtic_data_index =[]
    problem_data_index,bonds_list = apply_fill(bonds_list,problemtic_data_index)
    print("--- %s seconds ---" % (time.time() - start_time),'Next Step:Percentage Changed')

    ######form a list of tuple#####################################
    bond_spread_list = []
    for bond in bonds_list:
        try:
            bond = sliding_windows(bond, 90, start_date)
            bond['G_change'] = bond['GSpread'].pct_change()
            bond.fillna("-", inplace=True)
            if "-" not in bond.G_change.values[1:]:
                bond_spread_list.append((len(bond.GSpread.values),bond.SecurityID.iloc[0],bond))
            else:
                print(bond.SecurityID.iloc[0])
        except:
            pass
    print(len(bond_spread_list))
    #########################################################
    print("--- %s seconds ---" % (time.time() - start_time),'Next Step:Merging data')
    ##########################################################
    #for n in range(1,len(bond_spread_list)):
        #new_data = new_data.append(bond_spread_list[n][2])
    new_data = merg_sort(bond_spread_list)
    new_data = new_data[new_data["G_change"]!="-"]
    #print(len(new_data))
    #########################################################
    print("--- %s seconds ---" % (time.time() - start_time),'Next Step:Scale the data')
    ##########################################################
    ## Conduct a Min_Max Scale for the precentage change
    new_data[['G_change']] = sc.fit_transform(new_data[['G_change']], y=None)
    #print(new_data)

    final_data_2 = new_data.groupby('SecurityID')
    bonds_list_2 = [final_data_2.get_group(x) for x in final_data_2.groups]
    ############################################################
    print("--- %s seconds ---" % (time.time() - start_time),'Next Step: Warp Up Data')
    ############################################################
    #print(bonds_list_2[0])

    bond_spread_change_scaled_list = []

    for bond in bonds_list_2:
        bond_spread_change_scaled_list.append((len(bond.G_change.values), bond.SecurityID.iloc[0], bond.G_change.values[1:]))

    ####select the trading days with 90
    test_cluster_1 = select_data(windows, bond_spread_change_scaled_list)
    #test_cluster_1 = select_data(windows, bond_spread_list)
    test_cluster_1_with_spread = select_spread_data(test_cluster_1)
    print('Done')
    return bond_spread_change_scaled_list, test_cluster_1_with_spread
   # return bond_spread_list, test_cluster_1_with_spread