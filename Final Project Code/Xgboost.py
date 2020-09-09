###Import Library
import pandas as pd
import numpy as np
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import xgboost as xgb
from sklearn.metrics import mean_squared_error
import xgboost as xgb
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import Normalizer
from sklearn.model_selection import train_test_split
import math
import datetime

##### Read Preprocessed Data & Preprocess data for feature Regression
data = pd.read_csv('Security_and_market_movement_unscaled_cluster125.csv')
### Drop the 'Unnamed: 0'
data.drop(columns=['Unnamed: 0'],inplace=True)

### Change the Bond's G_spread_change to basepoint which * 10000
data['cluster_G_change'] = data['cluster_G_change'] * (10000)
data['Target_G_change'] = data['Target_G_change'] * (10000)

#### Drop a duplicate columns
data.drop(columns=['G_change'],inplace=True)

#### Rename the Percentage of Type of Bond in the Cluster
data = data.rename(columns={ 'Jr Subordinated Unsecured':'Cluster Perecentage of Jr Subordinated Unsecured',
                     'Secured':'Cluster Perecentage of Secured',
                     'Sr Unsecured':'Cluster Perecentage of Unsecured',
                     'Subordinated Unsecured':'Cluster Perecentage of Subordinated Unsecured',
                     '1st Lien Secured':'Cluster Perecentage of 1st Lien Secured',
                     '2nd Lien Secured':'Cluster Perecentage of 2nd Lien Secured',
                     '3rd Lien Secured':'Cluster Perecentage of 3rd Lien Secured',
                     '1st lien':'Cluster Perecentage of 1st Lien',
                     'Asset Backed':'Cluster Perecentage of Asset Backed'})

### Remove the moving average of the market data
data_1 = data.drop(columns = ['skew_change_Weekly','swap_change_Weekly','vix_change_Weekly','Close_change_Weekly','slope_change_Weekly','AmtOutstanding'])

### Read Preprocessed dummy variable
dummy_variable = pd.read_csv('Security_info_post_dummy.csv')
dummy_variable.drop(columns=['Unnamed: 0'],inplace=True)
### Merge the dummy variable to the market&security change
data_1 = data_1.merge(dummy_variable,on=['SecurityID'],how='left')

###Select a given period:
data_1['KeyDate'] = pd.to_datetime(data_1['KeyDate'])
data_1 = data_1[data_1['KeyDate']<= datetime.datetime(2019,2,28)]