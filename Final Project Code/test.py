from utilitie import *
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from Preprocess import *
import time

start_time = time.time()
security_data = pd.read_csv("C:/Users/y437l/OneDrive/MMAI/Capstone/Data/SecurityData - Copy-LAPTOP-OAEJRPE8.csv")
security_data = security_data[['SecurityID', 'Currency', 'IssueDate', 'MaturityDate']]
### merge spread data togther
sc = MinMaxScaler()
data = pd.read_csv("C:/Users/y437l/OneDrive/MMAI/Capstone/Data/1.csv")
# file_lists = ['14426.csv','24001.csv','36128.csv','48087.csv','55086.csv']
# file_lists = ['14426.csv']
###left join the currency data into the spread data
# final_data = merge_data(file_lists,data)
final_data = data
final_data = final_data.merge(security_data, on=['SecurityID'], how='left')
final_data = sliding_windows(final_data, 90, '2018-12-31')
print(1, len(final_data))
# final_data = final_data.dropna(subset=['ZSpread'])
security_data.drop_duplicates(subset="SecurityID", keep='first', inplace=True)
# final_data.drop_duplicates(subset =["SecurityID",'KeyDate'], keep = 'first', inplace = True)
print(2, len(final_data))
###select currency as USD
final_data_1 = final_data[final_data.Currency == 'USD']
final_data = final_data_1.groupby('SecurityID')
print(final_data)
bonds_list = [final_data.get_group(x) for x in final_data.groups]
######apply fill missing value function to bonds_list:
print("--- %s seconds ---" % (time.time() - start_time))
problemtic_data_index = []
problem_data_index, bonds_list = apply_fill(bonds_list, problemtic_data_index)
print("--- %s seconds ---" % (time.time() - start_time))
print(len(bonds_list))
######form a list of tuple
bond_spread_list = []
for bond in bonds_list:
    try:
        bond['G_change'] = bond['GSpread'].pct_change()
        bond_spread_list.append((len(bond.GSpread.values), bond.SecurityID.iloc[0], bond))
    except:
        print(bond)
        pass
print("--- %s seconds ---" % (time.time() - start_time))
######
new_data = bond_spread_list[0][2]
for n in range(1, len(bond_spread_list)):
    new_data = new_data.append(bond_spread_list[n][2])

print("--- %s seconds ---" % (time.time() - start_time))
new_data[['G_change']] = sc.fit_transform(new_data[['G_change']], y=None)
print(new_data)

final_data_2 = new_data.groupby('SecurityID')
bonds_list_2 = [final_data_2.get_group(x) for x in final_data_2.groups]

print("--- %s seconds ---" % (time.time() - start_time))
print(bonds_list_2[0])

bond_spread_change_scaled_list = []

for bond in bonds_list_2:
    bond_spread_change_scaled_list.append((len(bond.G_change.values), bond.SecurityID.iloc[0], bond.G_change.values[1:]))
print("--- %s seconds ---" % (time.time() - start_time))
####select the trading days with 90
test_cluster_1 = select_data(windows, bond_spread_change_scaled_list)
test_cluster_1_with_spread = select_spread_data(test_cluster_1)
print('Done')