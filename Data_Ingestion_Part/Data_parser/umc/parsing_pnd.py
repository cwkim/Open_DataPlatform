#-*- coding: utf-8 -*-
"""
    Author: Changwoo Kim (KETI) / cwkim@keti.re.kr
    parsing_pnd.py: parsing info for PND of Thinkware
"""
from pymongo import MongoClient, errors
import sys, os, time, logging
import datetime, pprint
import csv, json
import pandas as pd
import numpy as np
import zipfile

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

#def convert_TIME(ts):
#    fmt = "%Y-%m-%dT%H:%M:%SZ"
#    return datetime.datetime.fromtimestamp(int(ts)).strftime(fmt)    

def convert_dataframe(chunk):
    index_num = 0
    # GPS_DT, GPS_TIME
    gps_dt = list()
    gps_time = list()
    fmt_dt = "%Y%m%d"
    fmt_time = "%H%M%S"
    for ts in chunk['TS']:
        tmp_datetime = datetime.datetime.fromtimestamp(int(ts))
        gps_dt.append(str(tmp_datetime.strftime(fmt_dt)))
        gps_time.append(str(tmp_datetime.strftime(fmt_time)))
    chunk.insert(loc=index_num, column='GPS_DT', value=gps_dt)
    chunk.insert(loc=index_num, column='GPS_TIME', value=gps_time)
    # GPS_LAT
    gps_lat = list()
    for lat in chunk['LAT']:
        tmp_lat = np.float64(str(lat)[:2]+'.'+str(lat)[2:])
        gps_lat.append(tmp_lat)
    chunk.insert(loc=index_num, column='GPS_LAT', value=gps_lat)
    # GPS_LONG
    gps_lon = list()
    for lon in chunk['LON']:
        tmp_lon = np.float64(str(lon)[:3]+'.'+str(lon)[3:])
        gps_lon.append(tmp_lon)
    chunk.insert(loc=index_num, column='GPS_LONG', value=gps_lon)
    # Drop columns replaced
    chunk = chunk.drop(columns=['TS', 'LAT', 'LON'])
    return chunk

if __name__ == '__main__':
    logging.basicConfig(filename='parsing_pnd.log', level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    reload(sys)
    sys.setdefaultencoding('utf-8')

    file_dir = os.getcwd() + '/PND/files/'
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
    pnd_files = []
    for dirpath, dirnames, filenames in os.walk(file_dir):
        for filename in filenames:
            if filename.split('.')[-1] == 'csv':
                pnd_files.append(os.path.join(dirpath, filename))
            if filename.split('.')[-1] == 'txt':
                pnd_files.append(os.path.join(dirpath, filename))

    # Connect MongoDB
    client = mongodb_connect(url='', id='', pw='', initdb='')
    db = client.umc # Database name: umc
    collection = db.PND # Collection name: PND

    # [Column names changed] SPD to DRIVE_SPEED, ANGLE to GPS_ANGLE
    pnd_keys = ['PATH_ID', 'TS', 'LAT', 'LON', 'DRIVE_SPEED', 'GPS_ANGLE']  
    for pnd_file in pnd_files:
        try:
            count = 0
            for chunk in pd.read_csv(pnd_file, chunksize=1000000, names=pnd_keys, encoding='CP949'):
                chunk = convert_dataframe(chunk)
                if pnd_file.split(file_dir)[1].split('.')[-1] == 'csv': 
                    chunk["DATANAME"] = pnd_file.split(file_dir)[1].split('.csv')[0]
                elif pnd_file.split(file_dir)[1].split('.')[-1] == 'txt':
                    chunk["DATANAME"] = pnd_file.split(file_dir)[1].split('.txt')[0]
                else:
                    logging.error("Error %s : We cannot figure out type of the file (%s) ", err, pnd_file)
                chunk_json = json.loads(chunk.to_json(orient='records'))
                collection.insert(chunk_json)
                logging.info("chunk %s of file name %s uploaded ...", str(count), pnd_file)
                count += 1
        except Exception as err:
            logging.error("Error (%s) of file name %s was found ...", err, pnd_file)

    logging.info("All data files saved ...")

