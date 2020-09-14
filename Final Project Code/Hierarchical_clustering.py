## Using Hierarchical method to cluster the data
import pandas as pd
from Preprocess import *
import math
import datetime

###################
###security data###
###################
####read security data:
db_connection = sql.connect(host='0.0.0.0', database='bond_db', user='root', password='password')
security_query = "select * from security_info"
security_data = pd.read_sql(security_query, con=db_connection)


