#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import pymongo
import pandas as pd
import sys
import time
from datetime import datetime
import argparse


def brush_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", type=str, required=False, default='keti-ev.iptime.org', help="DB server ip to query")
    parser.add_argument("-port", type=int, required=False, default='27017', help="DB server port to query")
    parser.add_argument("-user", type=str, required=False, default='strapi', help="DB server username to query")
    parser.add_argument("-pw", type=str, required=False, default='strapi', help="DB server password to query")
    parser.add_argument("-dbname", type=str, required=False, default='testdb', help="MongoDB database name to query")
    parser.add_argument("-time_col", type=str, required=False, default='RECORD_TIME', help="csv time column name")
    parser.add_argument("-id_col", type=str, required=False, default='PHONE_NUM', help="csv id column name")

    args = parser.parse_args()
    
    return args


def recursive_search_dir(_nowDir, _filelist):
    dir_list = []  # 현재 디렉토리의 서브디렉토리가 담길 list
    f_list = os.listdir(_nowDir)
    # print(" [loop] recursive searching ", _nowDir)

    for fname in f_list:

        file_extension = os.path.splitext(fname)[1]  # 파일 확장자
        if os.path.isdir(_nowDir + "/" + fname):
            dir_list.append(_nowDir + "/" + fname)
        elif os.path.isfile(_nowDir + "/" + fname):
            if file_extension == ".csv" or file_extension == ".CSV":
                _filelist.append(_nowDir + "/" + fname)

    for toDir in dir_list:
        recursive_search_dir(toDir, _filelist)


# mongodb에 연결
def connect_mongo(MONGO_HOST, MONGO_PORT, USER_NAME, USER_PASS, DB_NAME, coll_name):
    try:
        client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT, username=USER_NAME, password=USER_PASS, connect=False)
        db = client[DB_NAME]
        return db[coll_name]
    except Exception as e:
        print('DB connect error!')
        print(e)
        sys.exit(1)


# 몽고DB에 저장
def save_to_mongo(_data, MONGO_HOST, MONGO_PORT, USER_NAME, USER_PASS, DB_NAME, coll_name):
    collection = connect_mongo(MONGO_HOST, MONGO_PORT, USER_NAME, USER_PASS, DB_NAME, coll_name)
    try:
        result = collection.insert_many(_data)
        print(
            '%d rows 저장 완료 ( document : "%s",  collection : "%s" )' % (len(result.inserted_ids), DB_NAME, coll_name)
        )

    except Exception as e:
        print(e)
        sys.exit(1)


def get_coll_name(_dict_list, _TIME_COL_NAME):
    csv_date = _dict_list[0][_TIME_COL_NAME]
    coll_name = csv_date[0:4] + csv_date[5:7] + csv_date[8:10]
    return coll_name


def change_id_field_format(_df, _ID_COL_NAME):
    id_list = _df[_ID_COL_NAME].to_list()
    str_id_list = [str(_id) for _id in id_list]
    _df[_ID_COL_NAME] = str_id_list    


if __name__ == "__main__":

    _args = brush_argparse()

    CSV_DIR = "../CSV_in"
    MONGO_HOST = _args.ip
    MONGO_PORT = _args.port
    USER_NAME = _args.user
    USER_PASS = _args.pw
    DB_NAME = _args.dbname
    TIME_COL_NAME = _args.time_col
    ID_COL_NAME = _args.id_col

    s = time.time()

    file_list = []

    if os.path.isdir(CSV_DIR):
        recursive_search_dir(CSV_DIR, file_list)
        file_list.sort()

    length = 0
    for _filepath in file_list:
        try:
            print("reading [%s]" % _filepath)

            try:
                # df1은 위에 두 줄 스킵, df2는 위에 두 줄만
                df = pd.read_csv(_filepath, low_memory=False, encoding="utf-8")
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(_filepath, low_memory=False, encoding="euc-kr")
                except UnicodeDecodeError:
                    df = pd.read_csv(_filepath, low_memory=False, encoding="cp949")

            change_id_field_format(df, ID_COL_NAME)

            data_dict_list = df.to_dict(orient="records")

            print("변환 완료, 레코드 수 : %d rows" % len(data_dict_list))
            if len(data_dict_list) == 0:
                continue
            
            coll_name = get_coll_name(data_dict_list, TIME_COL_NAME)
            
            save_to_mongo(data_dict_list, MONGO_HOST, MONGO_PORT, USER_NAME, USER_PASS, DB_NAME, coll_name)
            length += 1
        except Exception as e:
            print(_filepath + " 에러 발생")
            print(e)
            continue

    print("총 전송 파일 수 : " + str(length))
    print("total run time : " + str(time.time() - s))