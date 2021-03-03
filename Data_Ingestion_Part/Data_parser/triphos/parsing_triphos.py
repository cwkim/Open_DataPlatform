#-*- coding: utf-8 -*-
"""
    Author: Changwoo Kim (KETI) / cwkim@keti.re.kr
    parsing_triphos.py: parsing Triphos log files to MongoDB for Collection
"""
from pymongo import MongoClient, errors
import sys, os, time, logging
import csv, json, pprint
import pandas as pd
import numpy as np
import zipfile
from datetime import timedelta, datetime
from pytz import timezone

KST = timezone('Asia/Seoul')

# mongodb_connect: Local access to collection MongoDB
def mongodb_connect(url, id, pw, initdb):
    try:
        client = MongoClient(url, username=id, password=pw, authSource=initdb, connect=False)
        return client
    except errors.ServerSelectionTimeoutError as err:
        logging.error("Failed to connect to server {}".format(server_config))

def filename(filename, filetype):
    if filetype == 'zip':
        fn = filename.split('.zip')[0]
    if filetype == 'csv':
        fn = filename.split('.csv')[0]
    return fn

def convert_standard_df(df):
    columns_mapper = { "COMP_IDX":"COMPANY_IDX", 
                       "CAR_IDX":"VEHICLE_IDX",
                       "RECV_DT_STR":"RECORD_TIME_KOR",
                       "RECV_DT":"RECORD_TIME",
                       "LATITUDE":"GPS_LAT",
                       "LONGITUDE":"GPS_LONG",
                       "DTL_ADDR":"ADDRESS",
                       "GPS_AZIMUTH":"GPS_ANGLE",
                       "SPEED":"DRIVE_SPEED",
                       "RPM":"RPM",
                       "DAY_MILEAGE":"DRIVE_LENGTH_DAY",
                       "TOTAL_MILEAGE":"DRIVE_LENGTH_TOTAL",
                       "CAR_STATUS":"CAR_STATUS",
                       "BATTERY_VOLT":"BATTERY_VOLTAGE",
                       "KEY_ON_DATE":"KEY_ON_DATE",
                       "TEMP1":"TEMPER1",
                       "TEMP2":"TEMPER2",
                       "DAY_OIL":"FUEL_CONSUM_DAY",
                       "TOTAL_OIL":"FUEL_CONSUM_TOTAL",
                       "SEC_COUNT":"SEC_COUNT",
                       "SEC_DATA":"SEC_DATA",
                       "INS_DT":"REQ_TIME",
                     }
    std_columns_types = {"COMPANY_IDX":int,
                         "VEHICLE_IDX":int,
                         "RECORD_TIME_KOR":str,
                         "RECORD_TIME":str,
                         "GPS_LAT":float,
                         "GPS_LONG":float,
                         "ADDRESS":str,
                         "GPS_ANGLE":float,
                         "DRIVE_SPEED":int,
                         "RPM":int,
                         "DRIVE_LENGTH_DAY":int,
                         "DRIVE_LENGTH_TOTAL":int,
                         "CAR_STATUS":str,
                         "BATTERY_VOLTAGE":float,
                         "KEY_ON_DATE":str,
                         "TEMPER1":float,
                         "TEMPER2":float,
                         "FUEL_CONSUM_DAY":int,
                         "FUEL_CONSUM_TOTAL":int,
                         "SEC_COUNT":int,
                         "SEC_DATA":str,
                         "REQ_TIME":str, 
                         "ORIGIN_TYPE":int,
    }
    # Rename column names for standard
    df = df.rename(columns=columns_mapper)
    # Add column for Triphos DTG type
    df["ORIGIN_TYPE"] = 2
    # Covert column types for standard
    df = df.astype(std_columns_types)
    # Change DataFrame from String to Datetime
    str2date_list = ["RECORD_TIME_KOR", "RECORD_TIME", "KEY_ON_DATE", "REQ_TIME"]
    for i in str2date_list:
        try:
            if len(df[i][df[i] == '              ']) > 0:
                # Replace blank values to NaN
                df[i] = df[i].replace('              ', None)
                #df[i] = df[i].replace(r'^\s*$', np.nan, regex=True)
            df[i] = pd.to_datetime(df[i])
            #pd.to_datetime(df[i][df[i] != '              '].dropna())
        except Exception as err:
            logging.error("Error (%s) of df[%s] in convert_standard_df method ...", err, i)
    return df
    

if __name__ == '__main__':
    logging.basicConfig(filename='parsing_triphos.log', level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    reload(sys)
    sys.setdefaultencoding('utf-8')

    file_dir = os.getcwd() + '/Triphos_DTG_DATA/files/'
    # Unzip zip files
    for f in os.listdir(file_dir):
        try:
            with open(file_dir+f, 'rb') as fp:
                if fp.readline(3) == 'PK\x03': # zip file head
                    fn = filename(f, 'zip')
                    fdir = file_dir+'/'+fn
                    if not os.path.isdir(fdir):
                        os.mkdir(fdir)
                        zf = zipfile.ZipFile(file_dir+f)
                        zf.extractall(fdir)
                        zf.close()
                        logging.info("file name %s upziped ...", f)
        except IOError as err:
            continue

    # Collecting csv type files only (csv style: <value>, <value>, <value>)
    raw_files = []
    for dirpath, dirnames, filenames in os.walk(file_dir):
        for filename in filenames:
            if filename.split('.')[-1] == 'csv':
                raw_files.append(os.path.join(dirpath, filename))
            if filename.split('.')[-1] == 'txt':
                raw_files.append(os.path.join(dirpath, filename))

    # Connect MongoDB
    client = mongodb_connect(url='', id='', pw='', initdb='')
    db = client.triphos # Mongo database name: triphos

    # [Column names changed] SPD to DRIVE_SPEED, ANGLE to GPS_ANGLE
    raw_files = sorted(raw_files)
    for raw_file in raw_files:
        try:
            count = 0
            for chunk in pd.read_csv(raw_file, chunksize=10000, header=0, encoding='CP949'):
                chunk = convert_standard_df(chunk)
                coll_name = raw_file.split(file_dir)[1].split('.')[0]
                db[coll_name].insert_many(chunk.to_dict('records'))
                logging.info("chunk %s of file name %s uploaded ...", str(count), raw_file)
                count += 1
                time.sleep(1)
        except Exception as err:
            logging.error("Error (%s) of file name %s was found ...", err, raw_file)

    logging.info("All data files saved ...")

