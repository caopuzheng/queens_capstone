import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import seaborn as sns
from tsfresh import extract_features
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import normalize,StandardScaler
##extract time series feature from the time series
def tsfresh_extract(new_data):
    sc = MinMaxScaler()
    week_number = max(new_data['SecurityID'].map(new_data['SecurityID'].value_counts()))
    new_data = new_data[new_data['SecurityID'].map(new_data['SecurityID'].value_counts())==week_number]
    fdata = extract_features(new_data[['SecurityID','KeyDate','G_change']],column_id="SecurityID", column_sort="KeyDate")
    fdata1 = fdata.dropna(axis='columns')
    fdata2 = fdata1.loc[:, ~(fdata1 == fdata1.iloc[0]).all()]
    data_scaled = sc.fit_transform(fdata2.loc[:,'G_change__abs_energy':'G_change__variation_coefficient'])
    data_scaled = normalize(data_scaled)
    return data_scaled,fdata1.reset_index()