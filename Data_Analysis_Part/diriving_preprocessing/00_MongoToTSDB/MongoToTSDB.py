# -*- coding:utf-8 -*-

from __future__ import print_function
import pprint
from pymongo import MongoClient
import time
import pandas as pd
import os
import sys
import math
import json
import requests

def printProgressBar(iteration, total, prefix = 'Progress', suffix = 'Complete',\
                      decimals = 1, length = 100, fill = '█'): 
    # 작업의 진행상황을 표시
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' %(prefix, bar, percent, suffix), end='\r')
    sys.stdout.flush()
    if iteration == total:
        print()
        
def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[11:13], _time[14:16], _time[17:19])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))

    return epoch

######################   MongoDB   #############################
def MongoDB_query(_database_name, _measurement_name,\
            _url, _username, _pw):
    client = MongoClient(_url, username=_username, password=_pw)
    db = client[_database_name]
    coll = db[_measurement_name]
    coll_tot_line = coll.count()
    elex_all = coll.find(no_cursor_timeout=True) #.skip(10000000).limit(10000000)

    return elex_all, coll_tot_line

######################   OpenTSDB   #############################
def put_data(_list, __opentsdb_url):
    headers = { 'content-type' : 'application/json' }
    sess = requests.Session()

    url = __opentsdb_url+str('/api/put')
    

    try:
        response = sess.post(url, data=json.dumps(_list), headers = headers)

        while response.status_code > 204:
            print("[Bad Request] Put status: %s" % (response.status_code))
            print("[Bad Request] we got bad request, Put will be restarted after 3 sec!\n")
            time.sleep(3)
            
            print("[Put]" + json.dumps(_list, ensure_ascii=False, indent=4))
            response = sess.post(url, data=json.dumps(_list), headers = headers)

        #print "[Put finish and out]"

    except Exception as e:
        print("[exception] : %s" % (e))

def put_data_form(_list, __opentsdb_url, metric_name, _ts_field, _id_field, _rm_fields):
    data_len = len(_list)
    _buffer = []
    count = 0
    loop_count = 0
    for data_dict in _list:
        ts = data_dict[_ts_field].strftime('%s')
        loop_count += 1

        temp_list = data_dict.keys()
        dict_keys = [item for item in temp_list if item not in _rm_fields]

        for _key in dict_keys:
            cv_data = dict()
            cv_data['metric'] = metric_name
            cv_data["tags"] = dict()

            cv_data['timestamp'] = ts
            cv_data["value"] = str(data_dict[_key]).encode("utf-8")

            cv_data["tags"]['PHONE_NUM'] = str(data_dict[_id_field]).encode("utf-8")
            cv_data["tags"]["columns"] = str(_key)

            count +=  1
            _buffer.append(cv_data)

            if count >= 50:
                put_data(_buffer, __opentsdb_url)
                _buffer = []
                count = 0

        printProgressBar(loop_count, data_len)

    if len(_buffer) != 0:
        '''
        마지막 50개 이하로 남는 경우 나머지 전부를 put 한다.
        '''
        put_data(_buffer, __opentsdb_url)

if __name__ =='__main__':
    mongo_database_list = ['elex', 'hanuri', 'umc', 'public']
    mongo_measurement_name = "201905"
    mongo_url = '125.140.110.217:27027'
    mongo_username = 'cschae'
    mongo_pw = 'cschae'
    opentsdb_url = "http://125.140.110.217:54242"
    metric = 'Elex_201905_test'
    timestamp_field = 'RECORD_TIME'
    id_field = 'PHONE_NUM'
    remove_field_list = ['_id', 'REQ_TIME', 'CHANGE_LEVER', 'RECORD_TIME']

    print('MongoDB connecting...')
    cursor, coll_tot_line = MongoDB_query(mongo_database_list[0], mongo_measurement_name,\
            mongo_url, mongo_username, mongo_pw)
    
    print('finish MongoDB connecting, starting put Data to TSDB')
    
    count = 0
    line_count = 0
    total = 100000
    _list = []

    for _dict in cursor:
        #### OpenTSDB에 put ####
        _list.append(_dict)
        count +=1
        line_count +=1
        printProgressBar(count, total)
        
        if count % total == 0:
            count = 0
            put_data_form(_list, opentsdb_url, metric, timestamp_field, id_field, remove_field_list)
            print("\nOpenTSDB에 누적 입력 line수 : %d\n" %line_count)
            _list = []
            print('\t')
