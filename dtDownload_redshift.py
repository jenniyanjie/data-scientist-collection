#!/usr/bin/python3
"""
connecting redshift using python to download data
and save data as dataframe or csv
@author: Jennifer
"""

from __future__ import print_function #python 3 support
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
#import cPickle as pickle #python2
import _pickle as pickle #python3
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--newdownload', action = 'store_true', 
                    dest='newdownload', default = False, 
                    help = 'download the new data')
args = parser.parse_args()
##############################################################################
# download data from redshift ################################################
##############################################################################
if args.newdownload:
    print('Fetching data...')   
    # request the following info from dwh engineer
    redshift_endpoint = 'datawarehouse.some_chars_here.region_name.redshift.amazonaws.com.'
    redshift_user = 'xxxx' #user_name
    redshift_pass = 'xxxxxxxx' # user_password
    port = 5439
    dbname = 'dwh' 
    # creat engine
    engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" \
    % (redshift_user, redshift_pass, redshift_endpoint, port, dbname)
    engine = create_engine(engine_string)
    # good to go!
    start = datetime.now()
    df = pd.read_sql_query("""
    SELECT
    *
    FROM shit_table
    """, engine)
    print('Query time: %s' % str(datetime.now() - start))   
    # save the data
    print('Saving query result...')
    with open('raw_'+str(datetime.now().date()) + '.pkl', 'wb') as op:
        pickle.dump(df, op, -1)
else: 
    # you don't wanna download same data multiple times while you are debuging code
    print('Loading query result...')
    with open('raw_'+str(datetime.now().date()) + '.pkl', 'rb') as ip:
        df = pickle.load(ip)
        
# you can also save the data to csv, but it's shitty slow if the data is huge
df.to_csv('../data/test.csv', sep = ',', index = False, encoding = 'utf-8')
