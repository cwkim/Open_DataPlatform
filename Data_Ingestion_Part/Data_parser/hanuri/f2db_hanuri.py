#-*- coding: utf-8 -*-
"""
    Author: Changwoo Kim (KETI) / cwkim@keti.re.kr
            ChulseoungChae (KETI) / ccs7006@naver.com
    f2db_hanuri.py: HANURI realtime DB collection to collection DB for formalization
"""
from pymongo import MongoClient, errors
import sys, os, time, logging
from logging import handlers
import pprint
from datetime import timedelta, datetime
from pytz import timezone
import re

KST = timezone('Asia/Seoul')

LOCAL_URL = ''
LOCAL_ID =''
LOCAL_PW =''
LOCAL_INITDB = ''
# DBs Info.
REPLSET_LIST = ["", ""]
REPLSET_NAME = ""

# aws_mongodb_replset_connect: AWS MongoDB access
def aws_mongodb_replset_connect(replset_list=REPLSET_LIST, replset_name=REPLSET_NAME, username=LOCAL_ID, password=LOCAL_PW, initdb=LOCAL_INITDB):
    try:
        client = MongoClient(replset_list, replicaSet=replset_name, username=username, password=password, authSource=initdb, connect=False)
        db = client.hanuri
        return db
    except errors.ServerSelectionTimeoutError as err:
        logging.error("Failed to connect to Mongo DB: %s", str(err))

# mongodb_connect: Local access to collection MongoDB
def mongodb_connect(url=LOCAL_URL, id=LOCAL_ID, pw=LOCAL_PW, initdb=LOCAL_INITDB):
    try:
        # Access to Collection MongoDB (AWS instance)
        client = MongoClient(url, username=id, password=pw, authSource=initdb, connect=False)
        db = client.hanuri
        return db
    except errors.ServerSelectionTimeoutError as err:
        logging.error("Failed to connect to server {}".format(server_config))

def change_keys(doc):
    # Predefined fields: 20 items (common_data_template_0.9.2_20180710.excel)
    fields = {'RECORD_TIME':str, 'ORIGIN_TYPE':int, \
              'VEHICLE_NUM':str,'PHONE_NUM':str, \
              'DRIVE_STATUS':str,'DRIVE_LENGTH_DAY':int, \
              'DRIVE_LENGTH_TOTAL':int,'DRIVE_SPEED':int, \
              'ADDRESS':str,'GPS_STATUS':str, \
              'GPS_DT':str,'GPS_TIME':str, 'GPS_LENGTH':int, \
              'GPS_LAT':float,'GPS_LONG':float,'GPS_ANGLE':float, \
              'DEVICE_STATUS_CD':int, \
              'FUEL_CONSUM_TOTAL':int, \
              'TEMPER1':int,'TEMPER2':int,}

    # Vendor defined fields: {common_key : vendor_key}
    hanuri_fields = {'RECORD_TIME':'infoOccurDttm', 'ORIGIN_TYPE':'mileagetype', \
              'VEHICLE_NUM':'vhclNo','PHONE_NUM':'mobileNo', \
              'DRIVE_STATUS':'powermode','DRIVE_LENGTH_DAY':'persecDailyTravelDist', \
              'DRIVE_LENGTH_TOTAL':'persecTotalTravelDist','DRIVE_SPEED':'carSpeed', \
              'ADDRESS':'oldAddr','GPS_STATUS':'gpsStatus', \
              'GPS_DT':'optDt','GPS_TIME':'infoOccurDttm','GPS_LENGTH':'gpsMileage', \
              'GPS_LAT':'coordWGS84Y','GPS_LONG':'coordWGS84X','GPS_ANGLE':'gpsAngle', \
              'DEVICE_STATUS_CD':'commStateCode', \
              'FUEL_CONSUM_TOTAL':'fuelConsume', \
              'TEMPER1':'temperature01','TEMPER2':'temperature02',}
    orgin_type = {'MDT':1,'DTG':2,'APP':3,'GPS':4}

    tmp_doc = dict()

    for k, v in doc.items():
        for ck, hk in hanuri_fields.items():
            if k == hk:
                if not isinstance(v, type(None)): # None value is ignored
                    try:
                        if not ((len(str(v)) == 0) or str(v).isspace()):
                            # Tranlate infoOccurDttm str to date
                            if hk == 'infoOccurDttm':
                                tmp_doc[ck] = KST.localize(datetime.strptime(str(v), "%Y%m%d%H%M%S"))
                            else:
                                tmp_doc[ck] = fields[ck](v)
                    except UnicodeEncodeError:
                        if isinstance(v, unicode):
                            tmp_doc[ck] = str(v.encode('utf8'))
                    except ValueError:
                        if v in orgin_type:
                            tmp_doc[ck] = orgin_type[str(v)]
                    except Exception as e:
                        logging.error("[Exception Error] Parsing error for key and value", e)
    #tmp_doc['VENDOR'] = 'hanuri' # vendor's name added     # not use

    first_col_name = hanuri_fields['RECORD_TIME']
    second_col_name = hanuri_fields['GPS_TIME']

    try:
        if first_col_name in doc:
            if type(doc[first_col_name]) == datetime:
                datetype_str = str(doc[first_col_name])[:10]
                collection_name = re.sub('[-]', '', datetype_str)
            elif type(doc[first_col_name]) == unicode:
                collection_name = doc[first_col_name][:6]
        else:
            datetype_str = str(doc[second_col_name])[:10]
            collection_name = re.sub('[-]', '', datetype_str)
    except:
        tmp_doc = doc
        collection_name = -1
    
    return tmp_doc, collection_name

def insertdb(db, doc, collection_name):
    tmp = 0
    while tmp < 100:
        try:
            tmp += 1
            collection = db[collection_name]
            collection.insert_one(doc)
            #results = collection.insert_one(doc)
            #logging.info(results.inserted_id)
            break
        except Exception as err:
            logging.error("insertdb error: {}".format(str(err)))
            time.sleep(10)

def collection_count(db, trynum):
    _count = 0
    try:
        collection_list = db.collection_names()
        # gathering collection counting info
        for coll_name in collection_list:
            if str(coll_name[:2]) == '20':
                _count += db[coll_name].count()
    except errors.AutoReconnect as err:
        time.sleep(30)
        if trynum < 10:
            trynum += 1
            _count, trynum = collection_count(db, trynum)
        else:
            logging.error("collection_count error: {}".format(err))
            raise errors.AutoReconnect("connection closed")
    return _count, trynum

def query_db(db, collection, before_count, after_count):
    #for doc in collection.find()[before_count:after_count]:
    for doc in collection.find({}, batch_size=1, no_cursor_timeout=True)[before_count:after_count]:
        doc, collection_name = change_keys(doc)
        if collection_name == -1:
            logging.error("[Exception Error] Could not find any keys for RECORD_TIME and GPS_TIME: {}".format(str(doc)))
            continue
        try:
            insertdb(db, doc, collection_name)
        except Exception as err:
            logging.error("insertdb error : {}...".format(err))
            with open(HISTORYFILE_PATH,'w+') as fp:
                after_count, trynum = collection_count(db=db, trynum=0)
                fp.write(str(after_count))
            time.sleep(10)
    return after_count

if __name__ == '__main__':
    logging.basicConfig(filename='f2db_hanuri.log', level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    LOG_MAX_BYTES = 10*1024*1024    # 10 MB
    handlers.RotatingFileHandler(filename='f2db_hanuri.log', maxBytes=LOG_MAX_BYTES, backupCount=5)

    HISTORYFILE_PATH = './history_num.txt' # logfile name for recording
    #db = mongodb_connect(url=LOCAL_URL, id=LOCAL_ID, pw=LOCAL_PW, initdb=LOCAL_INITDB)
    db = aws_mongodb_replset_connect(replset_list=REPLSET_LIST, replset_name=REPLSET_NAME, username=LOCAL_ID, password=LOCAL_PW, initdb=LOCAL_INITDB)
    collection = db.realtime
    if not os.path.exists(HISTORYFILE_PATH):
        with open(HISTORYFILE_PATH,'w') as fp:
            before_count, trynum = collection_count(db=db, trynum=0)
            fp.write(str(before_count))
    else:
        with open(HISTORYFILE_PATH, 'r') as fp:
            before_count = int(fp.read())

    while True:
        after_count = collection.count()
        if after_count > before_count:
            before_count = query_db(db, collection, before_count, after_count)
            with open(HISTORYFILE_PATH,'w+') as fp:
                fp.write(str(before_count))
        time.sleep(5) # pause 5 sec
