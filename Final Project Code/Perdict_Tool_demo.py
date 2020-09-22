import pandas as pd
import numpy
from feature_extraction import tsfresh_extract
import mysql.connector as sql
import time
import datetime

##### Working on#####
class perdict_tool:
	def main():
		SecurityID = input('Enter your SecurityID:')
		KeyDate = input('Enter Your KeyDate')
		###Split KeyDate
		Key_Date_list = KeyDate.split('-')
		Key_date_datetime = datetime.datetime(Key_Date_list[0],Key_Date_list[1],Key_Date_list[2])
		#####Create engine:
		#db_connection = sql.connect(host='0.0.0.0', database='bond_db', user='root', password='password')
		##bond query
		#bond_query = "select * from bond_spread where SecurityID ={} and KeyDate='{}' ".format(SecurityID,KeyDate)
		#bond_spread_data = pd.read_sql(bond_query,con= db_connection)
		data = pd.read_csv('Data/Security_and_market_movement_unscaled_cluster125.csv')
		###import the cluster:

		###import the scaler:

		###import the perdicting model:

	def retry(f, seconds, pause=0):
		start = time.time()
		stop = start + seconds
		attempts = 0
		result = 'failed'

		while True:
			if time.time() < stop:
				try:
					f()
				except Exception as e:
					attempts += 1
					print(e)
					time.sleep(pause)
					print
					'Restarting!'
					continue
				else:
					result = 'succeeded'
			print
			'%s after %i attempts.' % (result, attempts)
			break

	if __name__ == "__main__":
		retry(main, 5, 0.1)