from Regression_Data_Preprocessing import Regression_Preprocessing
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error,r2_score,mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor
import pickle


###Change Your Weekly or Daily accordingly here:
data = Regression_Preprocessing(pd.read_csv('Data/Security_and_market_movement_unscaled_cluster_weekly_125_2019_02_28.csv'))

###Get X and Y from the data:
X,Y = data.Preprocessing()

### Split the X,Y to training and testing data:
X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.33, random_state=10)

### Nerual Network Regression
regr = MLPRegressor(random_state=42, max_iter=50).fit(X_train, y_train)

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
mae_test = mean_absolute_error(y_test, preds)
mae_train = mean_absolute_error(y_train,pred_train)
print("mae Test:{}".format(mae_test))
print("mae Train:{}".format(mae_train))

pickle.dump(regr, open("Neural_Network.pkl", "wb"))
print('Saved Neural NetWork Model, Done')
regr.get_params()