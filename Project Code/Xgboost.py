from Regression_Data_Preprocessing import Regression_Preprocessing
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error,r2_score,mean_absolute_error
import xgboost as xgb
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

###Change Your Weekly or Daily accordingly here:
data = Regression_Preprocessing(pd.read_csv('Data/Security_and_market_movement_unscaled_cluster_weekly_125_v2.csv'))

###Get X and Y from the data:
X,Y = data.Preprocessing()

### Split the X,Y to training and testing data:
X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.33, random_state=10)

### set up the the xgb regression:
xg_reg = xgb.XGBRegressor(objective ='reg:squarederror', colsample_bytree = 0.1, learning_rate = 0.1,
                max_depth =100, alpha = 100, n_estimators = 100)

### Xgboost fit the X_train and Y_train:
xg_reg.fit(X_train,y_train)

### Predict the Training and Test set:
preds = xg_reg.predict(X_test)
pred_train= xg_reg.predict(X_train)

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

##Plot Feature importance:
xgb.plot_importance(xg_reg)
plt.rcParams['figure.facecolor'] = 'white'
plt.rcParams['figure.figsize'] = [500, 500]
plt.rcParams.update({'font.size': 34})
plt.title('Feature importance')
plt.xlabel('F1')
plt.ylabel('Feature')
plt.show()

##Plot Xgboost Decision Tree:
import matplotlib.pyplot as plt
xgb.plot_tree(xg_reg,num_trees=0,rankdir='LR')
plt.rcParams['figure.figsize'] = [100, 100]
plt.show()