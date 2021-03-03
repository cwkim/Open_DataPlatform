#-*- coding: utf-8 -*-
"""
    Author: Changwoo Kim (KETI) / cwkim@keti.re.kr
    collecting_VIN.py: Uploading VIN(Vehicle Identification Number) data to Release Mongo DB
"""
from multiprocessing import Process, Queue, Pool
from pymongo import MongoClient, errors
import sys, os, time, logging
import datetime, pprint
import urllib, io, csv, json
import pandas as pd
import codecs

# mongodb_connect: Local access to collection MongoDB
def mongodb_connect(id, pw, ip, port):
    try:
        server_config = "mongodb://" + id + ':' + pw + '@' + ip + ':' + port + '/'
        client = MongoClient(server_config)
        return client
    except errors.ServerSelectionTimeoutError as err:
        logging.error("Failed to connect to server {}".format(server_config))

if __name__ == '__main__':
    #logging.basicConfig(filename='collecting_VIN.log', level=logging.INFO,
    #                    format='%(asctime)s - %(message)s',
    #                    datefmt='%Y-%m-%d %H:%M:%S')
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # Collecting excel files
    files_dir = os.getcwd() + '/VIN/'
    for dirpath, dirnames, filenames in os.walk(files_dir):
        for filename in filenames:
            if filename.split('.')[-1] == 'xlsx':
                filedir = os.path.join(dirpath, filename)

    # preprocessing columns
    xls = pd.ExcelFile(filedir)
    sheets = xls.sheet_names
    df = pd.read_excel(xls, sheets[0]) # Sheet1 

    # inserting data to MongoDB
    with mongodb_connect(id='', pw='', ip='', port='') as client:
        collection = client.public.VIN  # Database name: public, Collection name: VIN
        collection.insert_many(df.to_dict('records'))    

    #logging.info("All data files saved ...")
    print("All data files saved ...")
