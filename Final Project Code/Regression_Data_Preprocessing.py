###Import Library
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

class Regression_Preprocessing:
	def __init__(self, data):
		self.data = data

	def Preprocessing(self):
		### Drop the 'Unnamed: 0'
		self.data.drop(columns=['Unnamed: 0'],inplace=True)

		### Remove the moving average of the market data
		data_1 = self.data.drop(columns = ['YieldWorst','YieldWorst_change','ModifiedDuration_Plain_change','skew_change_Weekly','swap_change_Weekly','vix_change_Weekly','Close_change_Weekly','slope_change_Weekly'])

		### Read Preprocessed dummy variable
		dummy_variable = pd.read_csv('Data/Security_info_post_dummy.csv')
		dummy_variable.drop(columns=['Unnamed: 0'],inplace=True)

		##Dummy variable with Catagorical data type:
		dummy_variable_part1 = dummy_variable.drop(columns=['MinPiece', 'Term', 'RatingSP','MinIncrement'])

		##Dummy variable with scalable data:
		dummy_variable_part2 =dummy_variable[['SecurityID','MinPiece', 'Term', 'RatingSP','MinIncrement']]

		### Merge the scalable data of dummy variable to the market & security change
		data_1 = data_1.merge(dummy_variable_part2,on=['SecurityID'],how='left')

		### Fill the missing rating with the Cluster's average
		data_1["RatingSP"] = data_1['RatingSP'].fillna(data_1.groupby('Group')['RatingSP'].transform('mean'))

		### Transfer the KeyDate data from String to Datetime format
		data_1['KeyDate'] = pd.to_datetime(data_1['KeyDate'])

		### Select the columns from ModifiedDuration_Plain to RatingSP:
		final_regression_data = data_1.loc[:,'ModifiedDuration_Plain':'RatingSP']

		### filter out NA and Inf data
		final_regression_data.dropna(inplace=True)
		final_regression_data.replace([np.inf, -np.inf], np.NaN, inplace=True)
		final_regression_data.dropna(inplace=True)

		#######################################
		### Set the Target and the Features ###
		#######################################

		Y = final_regression_data['Target_G_change']
		X = final_regression_data.drop(columns =['Target_G_change','Group'])

		##Remove the Outliers, from 1th quantile to 99th quantile:
		removed_outliers = Y.between(Y.quantile(.01), Y.quantile(.99))

		##Select the head for the final regression
		final_regression_data_head = data_1[['SecurityID','KeyDate']]

		##Filter out the outliers index
		X = X[removed_outliers]
		Y = Y[removed_outliers]

		#Remeber the X's Columns and index
		columns = X.columns
		index = X.index

		##Conduct the standardlize transformation
		transformer = StandardScaler().fit(X)
		X = transformer.transform(X)

		##Transfer the X back with index and columns
		X = pd.DataFrame(data=X,  # values
		                 index=index,  # 1st column as index
		                 columns=columns)

		###Merge categorical dummy variable into the feature sets
		X = pd.merge(final_regression_data_head, X, left_index=True, right_index=True)
		X = X.merge(dummy_variable_part1,on='SecurityID',how='left')
		X = X.loc[:,'ModifiedDuration_Plain':'ZERO COUPON']
		return X,Y

