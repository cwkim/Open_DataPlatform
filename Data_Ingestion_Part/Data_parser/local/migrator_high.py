#-*- coding: utf-8 -*-
"""
    Author: Changwoo Kim (KETI) / cwkim@keti.re.kr 
    migrator_high.py: Migrate AWS MongoDB to Local MongoDB
"""
from multiprocessing import Process, Queue
from pymongo import MongoClient, errors
import ConfigParser
import sys, os, time, logging
from logging import handlers
import datetime, pprint
import json

# DBs Info.
REPLSET_LIST = ["", ""]
REPLSET_NAME = ""
LOCAL_URL = ''
LOCAL_ID =''
LOCAL_PW =''
LOCAL_INITDB = ''
# Unbounded dataset in Primary DB in AWS
MIGRATION_DBS = ['elex', 'hanuri', 'umc']

# aws_mongodb_replset_connect: AWS MongoDB access
def aws_mongodb_replset_connect(replset_list=REPLSET_LIST, replset_name=REPLSET_NAME):
    try:
        client = MongoClient(replset_list, replicaSet=replset_name, readPreference='secondaryPreferred', connect=False)
        return client
    except errors.ServerSelectionTimeoutError as err:
        logging.error("Failed to connect to Mongo DB: %s", str(err))

# mongodb_connect: Local MongoDB access
def mongodb_connect(url=LOCAL_URL, id=LOCAL_ID, pw=LOCAL_PW, initdb=LOCAL_INITDB):
    try:
        client = MongoClient(url, username=id, password=pw, authSource=initdb, connect=False)
        return client
    except errors.ServerSelectionTimeoutError as err:
        logging.error("Failed to connect to server {}".format(server_config))

# def get_config(cfg):
#     db_dict = dict()
#     sections = cfg.sections()
#     for section in sections:
#         db, coll = cfg.items(section)
#         if ',' in coll[-1]:
#             colls = list()
#             for col in coll[-1].split(','):
#                 colls.append(col.replace(" ", ""))
#         else:
#             colls = coll[-1]
#         db_dict[db[-1]] = colls
#     return db_dict

def get_db_dict():
    db_dict = dict()
    with aws_mongodb_replset_connect(replset_list=REPLSET_LIST, replset_name=REPLSET_NAME) as aws_mongo:
        db_list = aws_mongo.list_database_names()
        for db in db_list:
            for i in MIGRATION_DBS:
                if db == i:
                    if db == 'hanuri':
                        # Remove realtime collection of hanuri database
                        db_dict[db] = [i for i in aws_mongo[db].list_collection_names() if not i == 'realtime']
                    else:
                        db_dict[db] = aws_mongo[db].list_collection_names()
    return db_dict

def init_migrating_proc(db, col, aws_count, local_count):
    # Access to DBs due to "Create MongoClient only after forking"
    aws_mongo = aws_mongodb_replset_connect(replset_list=REPLSET_LIST, replset_name=REPLSET_NAME)
    local_mongo = mongodb_connect(url=LOCAL_URL, id=LOCAL_ID, pw=LOCAL_PW, initdb=LOCAL_INITDB)
    try:
        # Case: empty status
        if (local_count == int(0)) and (aws_count != int(0)):
            for doc in aws_mongo[db][col].find({}, batch_size=1, no_cursor_timeout=True)[:aws_count]:
                local_mongo[db][col].insert(doc)
            logging.info("[INFO][INIT] db: %s, col: %s, count: %s inserted ...", db, col, str(aws_count))
        # Case: full status
        elif local_count == aws_count:
            logging.info("[INFO][DONE] db: %s, col: %s, count: %s done ...", db, col, str(aws_count))
            pass
        # Case: residual status
        else:
            for doc in aws_mongo[db][col].find({}, batch_size=1, no_cursor_timeout=True)[local_count:aws_count]:
                local_mongo[db][col].insert(doc)
            logging.info("[INFO][INIT] db: %s, col: %s, count: %s inserted ...", db, col, str(aws_count))
    except Exception as err:
        logging.error("[ERROR] init_migrating_proc: {}".format(err))
        pass

def init_migrating(db_dict):
    aws_mongo = aws_mongodb_replset_connect(replset_list=REPLSET_LIST, replset_name=REPLSET_NAME)
    local_mongo = mongodb_connect(url=LOCAL_URL, id=LOCAL_ID, pw=LOCAL_PW, initdb=LOCAL_INITDB)
    try:
        procs = []
        for db in db_dict.keys():
            if isinstance(db_dict[db], type(list())):
                for col in db_dict[db]:
                    aws_count = aws_mongo[db][col].count()
                    local_count = local_mongo[db][col].count()
                    procs.append(Process(target=init_migrating_proc, args=(db, col, aws_count, local_count)))
                    time.sleep(1)
    except errors.AutoReconnect as err:
        time.sleep(10)
        logging.warn("init_migrating() Pymongo auto-reconnecting...: %s (waiting 10 secs)", str(err))
        pass
    
    # Multiple porcessing started
    for p in procs:
        p.start()
    # Multiple porcessing terminated
    for p in procs:
        p.join()

def realtime_migrating_proc(db, col, before_count, current_count):
    # Access to DBs due to "Create MongoClient only after forking"
    aws_mongo = aws_mongodb_replset_connect(replset_list=REPLSET_LIST, replset_name=REPLSET_NAME)
    local_mongo = mongodb_connect(url=LOCAL_URL, id=LOCAL_ID, pw=LOCAL_PW, initdb=LOCAL_INITDB)
    try:
        for doc in aws_mongo[db][col].find({}, batch_size=1, no_cursor_timeout=True)[before_count:current_count]:
            local_mongo[db][col].insert(doc)
        logging.info("[INFO][REALTIME] db: %s, col: %s, current_count: %s inserted ...", db, col, current_count)
    except Exception as err:
        logging.error("[ERROR] realtime_migrating_proc: {}".format(err)) 
        pass

def realtime_migrating(db_dict):
    # Access to DBs for counting
    aws_mongo = aws_mongodb_replset_connect(replset_list=REPLSET_LIST, replset_name=REPLSET_NAME)
    local_mongo = mongodb_connect(url=LOCAL_URL, id=LOCAL_ID, pw=LOCAL_PW, initdb=LOCAL_INITDB)
    try:
        procs = []
        for db in db_dict.keys():
            if isinstance(db_dict[db], type(list())):
                for col in db_dict[db]:
                    current_count = aws_mongo[db][col].count()
                    before_count = local_mongo[db][col].count()
                    if current_count > before_count:
                        procs.append(Process(target=realtime_migrating_proc, args=(db, col, before_count, current_count)))
                        time.sleep(1)
    except errors.AutoReconnect as err:
        time.sleep(10)
        logging.warn("realtime_migrating() Pymongo auto-reconnecting...: %s (waiting 10 secs)", str(err))
        pass

    # Multiple porcessing started
    for p in procs:
        p.start()
    # Multiple porcessing terminated
    for p in procs:
        p.join()

if __name__ == '__main__':
    logging.basicConfig(filename='migrator_high.log', level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    LOG_MAX_BYTES = 10*1024*1024    # 10 MB
    handlers.RotatingFileHandler(filename='migrator_high.log', maxBytes=LOG_MAX_BYTES, backupCount=5)

    # Initial migrating all databases of MongoDB in AWS
    init_migrating(db_dict=get_db_dict())
           
    # Interation for updating data
    while True:
        realtime_migrating(db_dict=get_db_dict())
        time.sleep(10) # Pause 10 secs

