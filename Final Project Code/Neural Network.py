from Regression_Data_Preprocessing import Regression_Preprocessing
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error,r2_score
import xgboost as xgb
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
from sklearn.neural_network import MLPRegressor
from sklearn.datasets import make_regression


###Change Your Weekly or Daily accordingly here:
data = Regression_Preprocessing(pd.read_csv('Data/Security_and_market_movement_unscaled_cluster125.csv'))

###Get X and Y from the data:
X,Y = data.Preprocessing()

### Split the X,Y to training and testing data:
X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.33, random_state=10)

### Nerual Network Regression
regr = MLPRegressor(random_state=42, max_iter=100).fit(X_train, y_train)

### Predict the Training and Test set:
preds = regr.predict(X_test)
pred_train= regr.predict(X_train)

###Evaluate the Prediction Result:
rmse_test = np.sqrt(mean_squared_error(y_test, preds))
rmse_train = np.sqrt(mean_squared_error(y_train, pred_train))
print("RMSE Test: %f" % (rmse_test))
print("RMSE Train: %f" % (rmse_train))
r2_test = r2_score(y_test, preds)
r2_train = r2_score(y_train,pred_train)
print("R2 Test:{}".format(r2_test))
print("R2 Train:{}".format(r2_train))

regr.get_params()