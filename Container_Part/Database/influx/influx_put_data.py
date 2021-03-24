# -*- coding: utf-8 -*-

import sys
import os
import time
from datetime import datetime
import argparse
import copy
import math
import json
from collections import OrderedDict
import pandas as pd
import influxdb


def isnan(value):
    try:
        return math.isnan(float(value))
    except:
        return False


def brush_args():
    
    _len = len(sys.argv)

    if _len < 5:
        print(" 추가 정보를 입력해 주세요. 위 usage 설명을 참고해 주십시오")
        print(" python 실행파일을 포함하여 아규먼트 갯수 9개 필요 ")
        print(" 현재 아규먼트 %s 개가 입력되었습니다." % (_len))
        print(" check *this_run.sh* file ")
        exit(" You need to input more arguments, please try again \n")

    _field = sys.argv[1]
    _ts = sys.argv[2]
    _carid = sys.argv[3]
    _ip = sys.argv[4]
    _port = sys.argv[5]
    _username = sys.argv[6]
    _password = sys.argv[7]
    _database_name = sys.argv[8]
    _measurement_name = sys.argv[9]
    

    return _field, _ts, _carid, _ip, _port, _username, _password, _database_name, _measurement_name


def pack_to_meta(field, ts, carid):
    ret = {}
    ret["field"] = field.split("|")
    ret["timestamp"] = ts
    ret["carid"] = carid
    return ret


def connect_influx(ip, port, username, password, database_name):
    try:
        client = influxdb.DataFrameClient(host=ip, port=port, username='cschae', password='evev2021', database =database_name)
    except Exception as e:
        print("DB connection error: ", e) 
        
    return client


def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" % (_time[8:10], _time[5:7], _time[:4], _time[11:13], _time[14:16], _time[17:19])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int(time.mktime(time.strptime(date_time, pattern)))
    return epoch


def recursive_search_dir(_nowDir, _filelist):
    dir_list = []  # 현재 디렉토리의 서브디렉토리가 담길 list
    f_list = os.listdir(_nowDir)
    print(" [loop] recursive searching ", _nowDir)

    for fname in f_list:
        file_extension = os.path.splitext(fname)[1]

        if file_extension == ".csv" or file_extension == ".CSV":  # csv
            _idx = -4
        elif file_extension == ".xlsx" or file_extension == ".XLSX":  # excel
            _idx = -5
        else:
            pass

        if os.path.isdir(_nowDir + "/" + fname):
            dir_list.append(_nowDir + "/" + fname)
        elif os.path.isfile(_nowDir + "/" + fname):
            if fname[_idx:] == file_extension:
                _filelist.append(_nowDir + "/" + fname)

    for toDir in dir_list:
        recursive_search_dir(toDir, _filelist)


def file2df(_filename, _field, _ts, _carid):
    
    f_extension = os.path.splitext(_filename)[1]
    col = _field + [_ts, _carid]
    print
    if f_extension == ".csv" or f_extension == ".CSV":
        try:
            chunks = pd.read_csv(_filename, usecols=col, low_memory=False, chunksize=10000, encoding="utf-8")

        except UnicodeDecodeError:
            try:
                chunks = pd.read_csv(_filename, usecols=col, low_memory=False, chunksize=10000, encoding="euc-kr")
            except UnicodeDecodeError:
                chunks = pd.read_csv(_filename, usecols=col, low_memory=False, chunksize=10000, encoding="cp949")

        df = pd.concat(chunks, ignore_index=True)



    elif f_extension == ".xlsx" or f_extension == ".XLSX":
        df = pd.read_excel(_filename, usecols=col, converters={ts: str})
    else:
        df = []
    _tmp = ", ".join(_field)

    print("\n\n****[Covert File: %s to Dataframe]****" % _filename)
    print("    File: %s\n    Field name: %s\n    DataFrame length: %d" % (_filename, _tmp, len(df)))
    print("************************************************************")
    return df


def put_data(conn, df, batch, measurement_name, count, meta):
    # df = df[meta["field"]]
    #print(df)
    df = df.set_index(meta['timestamp'])
    df.index = pd.to_datetime(df.index)

    carlist = df[meta["carid"]].unique()
    
    for i in carlist:
        filter_df = df.query(meta['carid']+' == "%s"' %i)
        del filter_df[meta['carid']]
        client.write_points(dataframe=filter_df, batch_size = batch, measurement=measurement_name, tags={'carid': i})

    count += len(df)
    return count


if __name__ == "__main__":
    field, ts, carid, ip, port, username, password, database_name, measurement_name = brush_args()
    file_dir = "./files"
    file_list = []
    batch = 1000

    meta = pack_to_meta(field, ts, carid)

    client = connect_influx(ip, port, username, password, database_name)

    recursive_search_dir(file_dir, file_list)

    print("\n--------------------file list--------------------")
    for f in file_list:
        _size = os.path.getsize(f) / (1024.0 * 1024.0)
        print("file: %s   size: %.3f (MB)" % (f, _size))
    print("\n--------------------------------------------------\n")

    start_time = time.time()

    count = 0
    for file_name in file_list:
        df = file2df(file_name, meta["field"], ts, carid)
        if len(df) == 0:
            continue
        else:
            count = put_data(client, df, batch, measurement_name, count, meta)

    print("Put data row num : %s" %count)
                
    end_time = time.time()
    print("total run time : %f" % (end_time - start_time))