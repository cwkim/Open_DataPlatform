# -*- coding:utf-8 -*-
'''
    Author : Jaekyu Lee, github : https://github.com/JaekyuLee
             JW Park, github : https://github.com/jwpark9010
'''

from __future__ import print_function
import pprint
from pymongo import MongoClient
import time
import pandas as pd
import os
import sys
import math
import numpy as np
from collections import Counter
from collections import OrderedDict
import argparse


def printProgressBar(iteration, total, prefix = 'Progress', suffix = 'Complete',\
                      decimals = 1, length = 70, fill = '█'): 
    # 작업의 진행상황을 표시
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    _string_out = '\r%s |%s| %s%% %s' %(prefix, bar, percent, suffix)
    sys.stdout.write(_string_out)
    sys.stdout.flush()
    if iteration == total:
        None
        #print()


def brush_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", type=str, required=False, default='125.140.110.217', help="DB server ip to query")
    parser.add_argument("-port", type=str, required=False, default='27027', help="DB server port to query")
    parser.add_argument("-username", type=str, required=False, default='cschae', help="DB server username to query")
    parser.add_argument("-password", type=str, required=False, default='cschae', help="DB server password to query")
    parser.add_argument("-db_name", type=str, required=False, default='hanuri', help="MongoDB database name to query")
    parser.add_argument("-collection_name", type=str, required=False, default='201910', help="MongoDB collection name to query")
    parser.add_argument("-id_field_name", type=str, required=False, default='PHONE_NUM', help="id column name to query")
    parser.add_argument("-id_count_num", type=int, required=False, default=10, help="id count num to query")

    args = parser.parse_args()
    
    return args


######################   MongoDB   #############################
def MongoDB_query(_ip, _port, _username, _password, _db_name, vehicle_num, skip_value, _collection_name):
    # Query to DB
    # select db name, car ID
    # 타겟 DB의 모든 데이터 요청

    client = MongoClient(_ip+':'+_port, username=_username, password=_password, connect=False)
    db = client[_db_name]
    coll = db[_collection_name]
    coll_tot_line = coll.count()

    print('[쿼리시작]', str(vehicle_num), '....waiting for DB server return')
    Elex_all = coll.find({"PHONE_NUM":str(vehicle_num)}, no_cursor_timeout=True).skip(skip_value)
    record_line = Elex_all.count()
    print('[쿼리결과]', str(vehicle_num)+' record_line 수: %s' %record_line)
    return Elex_all, record_line, coll_tot_line


def query_id_list(_ip, _port, _username, _password, _db_name, _collection_name, _id_field_name):
    client = MongoClient(_ip+':'+_port, username=_username, password=_password, authSource='admin', connect=False)
    db = client[_db_name]
    coll = db[_collection_name]

    pipelines = list()
    pipelines.append({'$group' : {'_id' : '$'+_id_field_name, 'count' : {'$sum' : 1}}})
    results = coll.aggregate(pipelines)
    
    temp_Phone_Count = Counter(dict())

    for result in results:
        phone_num = result['_id']
        temp_Phone_Count[phone_num] = result['count']

    sorted_dict = sorted(temp_Phone_Count.items(), key=lambda x: x[1], reverse=True)    

    _id_list = []
    for _tuple in sorted_dict:
        _id_list.append(_tuple[0])

    return _id_list


if __name__ =='__main__':
    _args_ = brush_argparse()

    ip = _args_.ip
    port = _args_.port
    username = _args_.username
    password = _args_.password
    db_name = _args_.db_name
    collection_list = _args_.collection_name.split('|')
    id_field_name = _args_.id_field_name
    id_count_num = _args_.id_count_num

    for collection_name in collection_list:
        total_carid_list = query_id_list(ip, port, username, password, db_name, collection_name, id_field_name)

        carid_list = total_carid_list[:id_count_num]
        carid_list = list(map(str, carid_list))
   
        print(carid_list)

        perCount = 0
        for Vechile_num in carid_list:
            perCount += 1
            print("[대상 기간 %s] 차량ID : %s   진행 갯수 %s / %s \n" %(collection_name, Vechile_num, perCount, len(carid_list)))
            count = 0

            ##### 시간측정 #####
            start_time = time.time()

            cursor, record_line, collection_total_line = MongoDB_query(ip, port, username, password, db_name, Vechile_num, 0, collection_name)

            print("\nCollection 전체 라인 수 : %s" %collection_total_line)

            _list = []

            for _dict in cursor:
                _list.append(_dict)

                count +=1
                printProgressBar(count+1, record_line)

            df = pd.DataFrame(_list)
            df = df.fillna("NaN")

            try:
                directory = os.getcwd()
                if not(os.path.isdir(directory+"/result_csv_file/"+collection_name+"/")):
                    os.makedirs(os.path.join(directory+"/result_csv_file/"+collection_name+"/"))
            except OSError as e:
                print("Failed to create directory!!!")
                raise
            df.to_csv("result_csv_file/"+collection_name+"/%s.csv" %str(Vechile_num), mode='w', encoding='utf-8')

            ##### 측정시간 출력 #####
            run_time = time.time() - start_time
            print ( "\n run time: %.4f (sec)\n" % (run_time) )