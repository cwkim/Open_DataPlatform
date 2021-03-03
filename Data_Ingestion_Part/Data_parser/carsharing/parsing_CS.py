#-*- coding: utf-8 -*-
"""
    Author: Changwoo Kim (KETI) / cwkim@keti.re.kr
    parsing_CS.py: Uploading CS data to Release Mongo DB
"""
from multiprocessing import Process, Queue, Pool
from pymongo import MongoClient, errors
import sys, os, time, logging
import urllib, io, csv, json, pprint
import pandas as pd
import codecs
from datetime import timedelta, datetime
from pytz import timezone
import re

#KST = timezone('Asia/Seoul')

# DB Info.
LOCAL_URL = ''
LOCAL_ID =''
LOCAL_PW =''
LOCAL_INITDB = ''

# mongodb_connect: Local access to collection MongoDB
def mongodb_connect(url=LOCAL_URL, id=LOCAL_ID, pw=LOCAL_PW, initdb=LOCAL_INITDB):
    try:
        client = MongoClient(url, username=id, password=pw, authSource=initdb, connect=False)
        db = client.carssum  # Database name: Carssum
        return db
    except errors.ServerSelectionTimeoutError as err:
        logging.error("Failed to connect to server {}".format(server_config))

def inserting(df, coll_name):
    db = mongodb_connect(url=LOCAL_URL, id=LOCAL_ID, pw=LOCAL_PW, initdb=LOCAL_INITDB)
    for i in range(0, len(df.index)):
        try:
            db[coll_name].insert(df[i:i+1].to_dict('records'))
        except OverflowError as err:
            logging.error("inserting error: df[{0}:{1}] / reason: {2}".format(str(i),str(i+1),str(err.message)))
            pass
    #db[coll_name].insert_many(df.to_dict('records'))
    logging.info("Inserted chunk ...")

def find_collname(file_dir):
    if 'CS_2017' in file_dir:
        coll_name = "SHARED_VEHICLE_"+file_dir.split('.xlsx')[0].split('carsum_rawdata/CS_2017/')[-1].split('_2017')[0]
    elif 'CS_2018' in file_dir:
        coll_name = "SHARED_VEHICLE_"+file_dir.split('.xlsx')[0].split('carsum_rawdata/CS_2018/카썸_차량데이터_')[-1]
        #coll_name = file_dir.split('.xlsx')[0].split('carsum_rawdata/CS_2018/')[-1]
    else:
        logging.error("file_collname error: check your files ...")
        raise
    return coll_name

if __name__ == '__main__':
    logging.basicConfig(filename='parsing_CS.log', level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # Collecting CS xlsx files only
    excel_files = []
    files_dir = os.getcwd() + '/carsum_rawdata/'
    for dirpath, dirnames, filenames in os.walk(files_dir):
        for filename in filenames:
            if filename.split('.')[-1] == 'xlsx':
                excel_files.append(os.path.join(dirpath, filename))
    
    print("Collected excel file list: ")
    for i in excel_files:
        print(i)
    
    for ef in excel_files:
        # Second row to be column name
        if 'CS_2017' in ef:
            headline = 0
        elif 'CS_2018' in ef:
            headline = 1
        else:
            logging.error("main error: check your files ...")
            raise
        logging.info("Start to file read: {}".format(str(ef)))
        df = pd.read_excel(ef, header=headline, sheet_name=0)
        # Convert Pandas column to Datetime
        df['CREATE_TIME'] = pd.to_datetime(df['CREATE_TIME'], format="%Y-%m-%d %H:%M:%S")
        #df['CREATE_TIME'] = df['CREATE_TIME'].dt.tz_localize('UTC').dt.tz_convert('Asia/Seoul')
        df['GPS_TIME'] = pd.to_datetime(df['GPS_TIME'], format="%Y-%m-%d %H:%M:%S")
        #df['GPS_TIME'] = df['GPS_TIME'].dt.tz_localize('UTC').dt.tz_convert('Asia/Seoul')
        coll_name = find_collname(file_dir=ef)
        inserting(df, coll_name)
        del df
        logging.info("file name {}.xlsx inserted ...".format(coll_name))
        time.sleep(10)
        
    logging.info("All data files saved ...")
