#!/usr/bin/python3
"""
connecting redshift using python to download data
and save data as dataframe or csv
@author: Jennifer
"""

from __future__ import print_function
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from time import time
import pandas as pd
try:
    import cPickle as pickle #python2
except:
    import _pickle as pickle #python3
import logging
import MySQLdb as mdb

def setup_logging(default_level=logging.WARNING):
    """
    Setup logging configuration
    """
    logging.basicConfig(format='%(asctime)s   %(message)s', level=default_level)
    return logging.getLogger('Check_ifa_existence')

logger = setup_logging(logging.INFO)

##############################################################################
# download data from redshift ################################################
##############################################################################
def download_dt_from_redshift(savedt = False):
    logger.info('Fetching data...')

    # request the following info from dwh engineer
    redshift_endpoint = 'datawarehouse.some_chars_here.region_name.redshift.amazonaws.com.'
    redshift_user = 'xxxx' #user_name
    redshift_pass = 'xxxxxxxx' # user_password
    port = 5439
    dbname = 'dwh'
    # creat connection
    con_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" \
    % (redshift_user, redshift_pass, redshift_endpoint, port, dbname)
    con = create_engine(con_string)
    # sql
    sql = """SELECT
            *
            FROM shit_table"""

    with con:
        start = time()
        df = pd.read_sql_query(sql, con = con)
        logger.info('query time: {0:0>8}'.format(timedelta(seconds = int(time() - start))))

    # do something transformation to the dataframe

    # save the dataframe
    if savedt:
        # Save result as csv, but it's shitty slow if the data is huge
        df.to_csv('./ifa.csv', sep = ',', index = False, encoding = 'utf-8')
        # dump the result
        with open('raw_'+str(datetime.now().date()) + '.pkl', 'wb') as op:
            pickle.dump(df, op, -1)
    logger.info('data has downloaded!')
    return df
    '''
    to load the pickle
    print('Loading query result...')
    with open('raw_'+str(datetime.now().date()) + '.pkl', 'rb') as ip:
        df = pickle.load(ip)
    '''
##############################################################################
# download data from mysql ###################################################
##############################################################################

def download_dt_from_mysql(savedt = False):
    '''
    output dataframe
    '''
    logger.info('Connecting to aoc_dwh...')
    # Connect to the MySQL instance
    db_host = '127.0.0.1' # host-ip
    db_name = 'XXXX'
    db_user = 'XXXX'
    db_pass = '**********'
    con = mdb.connect(db_host, db_user, db_pass, db_name)

    sql = """SELECT *
             FROM shit-table-1 s1
             LEFT JOIN shit-table-2 s2
             ON s1.f1 = s2.f1
             WHERE shit-field-1 is what I want
             AND shit-field-2 is what I want;"""  #--concat(curdate()-interval 8 day)
    # Create a pandas dataframe from the SQL query
    with con:
        df = pd.read_sql_query(sql, con=con)
    # do something on the dataframe
    df['ifa'] = 'ifa-' + df['ifa'].astype(str) # in dmp, the key is 'ifa-' + ifa

    # save the dataframe
    if savedt: # Save result as csv
        df.to_csv('./ifa.csv', sep = ',', index = False, encoding = 'utf-8')

    logger.info('data has downloaded!')
    return df