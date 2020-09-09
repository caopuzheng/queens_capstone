import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from Preprocess import Average_daily_Gspread_change_cluster

#####Transfer the list of cluster bond security ids to pandas dataframe
def transfer_to_list_of_pd(clusters_list):
	clusters_data_list = []
	for i in clusters_list:
		temp_data = pd.DataFrame(i,columns=['SecurityID', 'Sector', 'Industry', 'Sub_Industry', 'Seniority', 'Issuedby',
                                            'Country', 'Rating', 'AmountIssued', 'Coupon', 'IssueDate', 'MaturityDate',
                                            'BondTerm','Term_Type','Cupon_Type','Group'])
		clusters_data_list.append(temp_data)
	return clusters_data_list


##### Assign rating to bonds based on the rating rate.
def assign_rating(x):
    x = float(x)
    if x < 0:
        return 'NR'
    elif x<=13.5:
        return "Junk"
    elif x>13.5 and x<=16.5:
        return 'Lower medium grade'
    elif x>16.5 and x<=19.5:
        return 'Upper medium grade'
    elif x>19.5 and x<=22.5:
        return 'High grade'
    else:
        return 'Prime'

####Assign rating
def process_rating_data(data):
	# data = data[data['KeyDate']<=end_date]
	rating = data.groupby('SecurityID', as_index=False).mean()
	rating['RatingSP'] = rating['RatingSP'].apply(lambda x: assign_rating(x))
	return rating


####Grab the information based on each cluster
def grab_clusters(clusters,fdata,security_data,rating_data):
    temp_list = []
    for cluster_index in range(0,len(clusters)):
        temp_list2 = []
        for bond_index in clusters[cluster_index]:
            bond_id = fdata.iloc[bond_index].id
            bond_Sector = security_data[security_data['SecurityID']==bond_id].SecuritySector.iloc[0]
            bond_Industry = security_data[security_data['SecurityID'] == bond_id].SecurityIndustry.iloc[0]
            bond_sub_Industry = security_data[security_data['SecurityID'] == bond_id].SecuritySubIndustry.iloc[0]
            bond_Seniority = security_data[security_data['SecurityID'] == bond_id].Seniority.iloc[0]
            bond_Issuer =security_data[security_data['SecurityID'] == bond_id].Issuer.iloc[0]
            bond_countryrisk =security_data[security_data['SecurityID'] == bond_id].CountryRisk.iloc[0]
            bond_amountIssued =security_data[security_data['SecurityID'] == bond_id].AmountIssued.iloc[0]
            bond_coupon =security_data[security_data['SecurityID'] == bond_id].Coupon.iloc[0]
            bond_MaturityDate = security_data[security_data['SecurityID'] == bond_id].MaturityDate.iloc[0]
            bond_issuedate =security_data[security_data['SecurityID'] == bond_id].IssueDate.iloc[0]
            bond_cupontype = security_data[security_data['SecurityID'] == bond_id].CouponType.iloc[0]
            try:
                bond_term = int(bond_MaturityDate.year) - int(bond_issuedate.year)
            except:
                bond_term = 100
            if bond_term >= 10:
                bond_type_term = 'Long Term'
            elif bond_term < 10 and bond_term >= 5:
                bond_type_term = 'Mid Term'
            else:
                bond_type_term = 'Short Term'
            try:
                bond_rating=rating_data[rating_data['SecurityID'] == bond_id].RatingSP.iloc[0]
            except:
                bond_rating = 'Not Founded'
            bond_cluster = 'Cluster {}' .format(cluster_index)
            temp_tuple = (bond_id,bond_Sector,bond_Industry,bond_sub_Industry,bond_Seniority,bond_Issuer,bond_countryrisk,bond_rating,bond_amountIssued,bond_coupon,bond_issuedate,bond_MaturityDate,bond_term,bond_type_term,bond_cupontype,bond_cluster)
            temp_list2.append(temp_tuple)
        temp_list.append(temp_list2)
    return temp_list



#### transfer the array to the list
def create_cluster_list(cluster_index):
    temp_1 = []
    for i in np.unique(cluster_index):
        temp_2 = []
        for n in range(0,len(cluster_index)):
            if cluster_index[n] == i:
                temp_2.append(n)
        temp_1.append(temp_2)
    return temp_1

### collect cluster information
def collect_cluster_info(clusters_data_list,end_date):
    cluster_info = pd.DataFrame()
    cluster_data = clusters_data_list[0][['SecurityID','Group']]
    for i in range(1,len(clusters_data_list)):
        temp = clusters_data_list[i][['SecurityID','Group']]
        cluster_data = cluster_data.append(temp,ignore_index=True)
    cluster_info['SecurityID'] = cluster_data['SecurityID']
    cluster_info[end_date] = cluster_data['Group']
    return cluster_info

###dump data to database
def dump_data(data,database,table):
    engine = create_engine('mysql+pymysql://root:password@0.0.0.0:3306/{}'.format(database))
    data.to_sql(con=engine,name=table,if_exists='append',index=False)
    engine.dispose()
    print('Dump data to db {}, table {}'.format(database,table))


def dump_Gspread_change_per_cluster(silding_windows,table,cluster_data):
    for i in silding_windows:
        temp_data,temp_d = Average_daily_Gspread_change_cluster(i,cluster_data)
        dump_data(temp_data,'bond_db',table)

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