from datetime import datetime
import re
import pandas as pd
from sqlalchemy import create_engine


log_extract_data = []

# opening the log file
with open('bq-results-data.json', 'r') as log_file:
    for line in log_file:
        log_data = log_file.readline()
        log_data = log_data.split(',')
        event_date = re.search(r'\d+', log_data[0])
        event_date = datetime.strptime(event_date.group(), '%Y%m%d').date()
        event_date = event_date.strftime('%Y-%m-%d')
        user_id = log_data[14].split(':')[1].strip('""')
        user_id = re.search(r'^\w{0,32}', user_id).group()
        log_extract_data.append([event_date, user_id])

#Copy extracted data to a pandas dataframe
pd_data = pd.DataFrame(log_extract_data, columns= ['Date', 'User_id'])

#Create an Sql Connection engine for copying the data to a mysql database
connection_engine = create_engine("mysql://{user}:{pw}@localhost/{db}".format(user = 'your-username', pw = 'your-password', db = 'log_data' ))
#Copy pandas data to mysql database
pd_data.to_sql('log_info', con = connection_engine, if_exists= 'append', index = False)


