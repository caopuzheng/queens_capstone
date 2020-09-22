import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine
from utility import dump_data

##Standard and Poor's weekly change
GSPC = pd.read_csv('Data/^GSPC.csv')
GSPC['Close_change']=GSPC['Adj Close'].pct_change()
GSPC_Weekly = GSPC.dropna(subset =['Close_change'])
GSPC_Weekly = GSPC_Weekly[['Date','Close_change']]
GSPC_Weekly.Date = GSPC_Weekly.Date.astype(str)

###Standard and Poor's daily close change
GSPC_daily = pd.read_csv('/Users/yangli/OneDrive/MMAI/Capstone/Data/^GSPC_daily.csv')
GSPC_daily['Close_change']=GSPC_daily['Adj Close'].pct_change()
GSPC_daily = GSPC_daily[['Date','Close_change']]
GSPC_daily['Close_change_Weekly'] = GSPC_daily[['Date','Close_change']].rolling(window=5).mean()
GSPC_daily = GSPC_daily.dropna(subset =['Close_change'])
GSPC_daily.head()

###10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity Slope data weekly
slope = pd.read_csv('Data/T10Y2Y.csv')
slope['DATE'] = pd.to_datetime(slope['DATE'])
slope = slope[slope['DATE']>=datetime.datetime(2018,12,31)]
slope = slope.set_index('DATE')
logic = {'T10Y2Y'  : 'last'}
offset = pd.offsets.timedelta(days=-6)
slope = slope.resample('W', loffset=offset).apply(logic)
slope = slope[slope['T10Y2Y']!='.']
slope["T10Y2Y"] = pd.to_numeric(slope["T10Y2Y"])
slope['slope_change']=slope['T10Y2Y'].pct_change()
slope.reset_index(inplace=True)
slope_weekly = slope.dropna(subset =['slope_change'])
slope_weekly['Date'] = slope_weekly['DATE']
slope_weekly = slope_weekly[['Date','slope_change']]
slope_weekly.head()

##10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity Slope data daily
slope_daily = pd.read_csv('Data/T10Y2Y.csv')
slope_daily = slope_daily[slope_daily['T10Y2Y']!='.']
slope_daily["T10Y2Y"] = pd.to_numeric(slope_daily["T10Y2Y"])
slope_daily['slope_change']=slope_daily['T10Y2Y'].pct_change()
slope_daily = slope_daily.dropna(subset =['slope_change'])
slope_daily['Date'] = slope_daily['DATE']
slope_daily = slope_daily[['Date','slope_change']]
slope_daily['slope_change_Weekly'] = slope_daily[['Date','slope_change']].rolling(window=5).mean()
slope_daily.head()

###Market voalility index weekly
vixcurrent = pd.read_csv('Data/vixcurrent.csv')
vixcurrent['Date'] = pd.to_datetime(vixcurrent['Date'])
vixcurrent = vixcurrent[vixcurrent['Date']>=datetime.datetime(2018,12,31)]
vixcurrent_weekly = vixcurrent.set_index('Date')
logic = {'VIX Close'  : 'last'}
offset = pd.offsets.timedelta(days=-6)
vixcurrent_weekly = vixcurrent_weekly.resample('W', loffset=offset).apply(logic)
vixcurrent_weekly['vix_change']=vixcurrent_weekly['VIX Close'].pct_change()
vixcurrent_weekly.reset_index(inplace=True)
vixcurrent_weekly=vixcurrent_weekly.dropna(subset =['vix_change'])
vixcurrent_weekly = vixcurrent_weekly[['Date','vix_change']]
##Daily
vixcurrent['vix_change']=vixcurrent['VIX Close'].pct_change()
vixcurrent_daily= vixcurrent.dropna(subset =['vix_change'])
vixcurrent_daily = vixcurrent_daily[['Date','vix_change']]
vixcurrent_daily['vix_change_Weekly'] = vixcurrent_daily[['Date','vix_change']].rolling(window=5).mean()

#Skewd index:
Skew = pd.read_csv('Data/skewdailyprices.csv')

Skew['Date'] = pd.to_datetime(Skew['Date'])
Skew = Skew[Skew['Date']>=datetime.datetime(2018,12,31)]
Skew_weekly = Skew.set_index('Date')
logic = {'SKEW'  : 'last'}
offset = pd.offsets.timedelta(days=-6)
Skew_weekly = Skew_weekly.resample('W', loffset=offset).apply(logic)
Skew_weekly['skew_change']=Skew_weekly['SKEW'].pct_change()
Skew_weekly.reset_index(inplace=True)
Skew_weekly= Skew_weekly.dropna(subset =['skew_change'])
Skew_weekly = Skew_weekly[['Date','skew_change']]
##Daily
Skew['skew_change']=Skew['SKEW'].pct_change()
Skew_daily= Skew.dropna(subset =['skew_change'])
Skew_daily = Skew_daily[['Date','skew_change']]
Skew_daily['skew_change_Weekly'] = Skew_daily[['Date','skew_change']].rolling(window=5).mean()

##ICE SWAP DATA 3 Years
swap_rate = pd.read_csv('Data/ICERATES1100USD3Y.csv')
swap_rate['DATE'] = pd.to_datetime(swap_rate['DATE'])
swap_rate = swap_rate[swap_rate['DATE']>=datetime.datetime(2018,12,31)]
swap_rate_weekly = swap_rate.set_index('DATE')
logic = {'ICERATES1100USD3Y'  : 'last'}
offset = pd.offsets.timedelta(days=-6)
swap_rate_weekly = swap_rate_weekly.resample('W', loffset=offset).apply(logic)
swap_rate_weekly = swap_rate_weekly[swap_rate_weekly['ICERATES1100USD3Y']!='.']
swap_rate_weekly["ICERATES1100USD3Y"] = pd.to_numeric(swap_rate_weekly["ICERATES1100USD3Y"])
swap_rate_weekly['swap_change']=swap_rate_weekly['ICERATES1100USD3Y'].pct_change()
swap_rate_weekly.reset_index(inplace=True)
swap_rate_weekly = swap_rate_weekly.dropna(subset =['swap_change'])
swap_rate_weekly['Date'] = swap_rate_weekly['DATE']
swap_rate_weekly = swap_rate_weekly[['Date','swap_change']]
swap_rate = swap_rate[swap_rate['ICERATES1100USD3Y']!='.']
swap_rate["ICERATES1100USD3Y"] = pd.to_numeric(swap_rate["ICERATES1100USD3Y"])
swap_rate['swap_change']=swap_rate['ICERATES1100USD3Y'].pct_change()
###Daily
swap_rate_daily = swap_rate.dropna(subset =['swap_change'])
swap_rate_daily['Date'] = swap_rate_daily['DATE']
swap_rate_daily = swap_rate_daily[['Date','swap_change']]
swap_rate_daily['swap_change_Weekly'] = swap_rate_daily[['Date','swap_change']].rolling(window=5).mean()

####Merge all other data
final_market_data = Skew_daily.merge(swap_rate_daily,on='Date',how='left')
final_market_data = final_market_data.merge(vixcurrent_daily,on='Date',how='left')
final_market_data.Date = final_market_data.Date.astype(str)
slope_weekly.Date = slope_weekly.Date.astype(str)
final_market_data = final_market_data.merge(GSPC_daily,on='Date',how='right')
final_market_data = final_market_data.merge(slope_daily,on='Date',how='left')
final_market_data.Date = pd.to_datetime(final_market_data.Date)

final_market_data = pd.DataFrame(final_market_data.rename(columns={"Date": "KeyDate"}))
final_market_data = final_market_data.replace([np.inf, -np.inf], np.nan)
final_market_data.fillna(method='ffill').dropna()

#dump_data(final_market_data,'bond_db','market_data_weekly')

