#-*- coding: utf-8 -*-
"""
    Author: Changwoo Kim (KETI) / cwkim@keti.re.kr
    collecting_koroad.py: Reqeust information related with kosta.or.kr/data.go.kr using Restful API
"""
from pymongo import MongoClient, errors
import sys, os, time, logging
import datetime, pprint
import urllib, io, csv, json
import pandas as pd
import numpy as np

DOWNLOAD_URLS = {
    "pdestrians_jaywalking":"http://taas.koroad.or.kr/api/down/jaywalkingdown.jsp",
    "schoolzone_child":"http://taas.koroad.or.kr/api/down/schoolzonedown.jsp",
    "child":"http://taas.koroad.or.kr/api/down/childdown.jsp",
    "bicycle":"http://taas.koroad.or.kr/api/down/bicycledown.jsp",
    "oldman":"http://taas.koroad.or.kr/api/down/oldmandown.jsp",
    "death":"http://taas.koroad.or.kr/api/down/deathdown.jsp",
}
# For pdestrians_jaywalking.csv, schoolzone_child.csv, child.csv, bicycle.csv, oldman.csv
#ACCI_ORI_COLUMNS = ["다발지식별자", "다발지그룹식별자", "법정동코드", "스팟코드", "관할경찰서", "다발지명", "발생건수", "사상자수", "사망자수", "중상자수", "경상자수", "부상신고자수", "경도", "위도", "다발지역폴리곤"]
ACCI_NEW_COLUMNS = ["ID", "DATE", "DISTRICT_CODE", "SPOT_CODE", "PRECINCT", "LOCATION", "NUM_OCCUR", "NUM_CASUALTY", "NUM_DEATH", "NUM_SEVERE", "NUM_SLIGHTLY", "NUM_INJURY", "GPS_LONG", "GPS_LAT", "POLYGON"]
# For death.csv
#INFO_ORI_COLUMNS = ["발생년", "발생년월일시", "발생분", "주야", "요일", "사망자수", "사상자수", "중상자수", "경상자수", "부상신고자수", "발생지시도", "발생지시군구", "사고유형_대분류", "사고유형_중분류", "사고유형", "법규위반_대분류", "법규위반", "도로형태_대분류", "도로형태", "당사자종별_1당_대분류", "당사자종별_1당", "당사자종별_2당_대분류", "당사자종별_2당", "발생위치X_UTMK", "발생위치Y_UTMK", "경도", "위도"]
INFO_NEW_COLUMNS = ["YEAR", "DATETIME", "MINUTES", "DAY_NIGHT", "DAY", "NUM_DEATH", "NUM_CASUALTY", "NUM_SEVERE", "NUM_SLIGHTLY", "NUM_INJURY", "SIDO", "GUGUN", "L_ACCITYPE", "M_ACCITYPE", "S_ACCITYPE", "L_VIOLTYPE", "S_VIOLTYPE", "L_ROADTYPE", "S_ROADTYPE", "L_CARTYPE1", "S_CARTYPE1", "L_CARTYPE2", "S_CARTYPE2", "X_UTMK", "Y_UTMK", "GPS_LONG", "GPS_LAT"]
FILE_MAP = {"pdestrians_jaywalking":"보행자무단횡단사고", "schoolzone_child":"스쿨존내어린이사고",\
            "child":"보행어린이사고", "bicycle":"자전거사고", "oldman": "보행노인사고", \
            "death":"사망교통사고"}

# mongodb_connect: Local access to collection MongoDB
def mongodb_connect(id, pw, ip, port):
    try:
        # Access to Collection MongoDB (AWS instance)
        server_config = "mongodb://" + id + ':' + pw + '@' + ip + ':' + port + '/'
        client = MongoClient(server_config)
        #client.server_info()
        return client
    except errors.ServerSelectionTimeoutError as err:
        logging.error("Failed to connect to server {}".format(server_config))

def mapping(f):
    fn = f.split('.csv')[0]
    kor_fn = FILE_MAP[fn]
    return kor_fn 

def to_float64(df, colname):
    try:
        if df.dtypes[colname] != np.float64: 
            df[colname] = df[colname].astype(np.float64)
    except Exception as err:
        for i in range(1, len(df[colname])+1):
            try:
                np.float64(df[colname].loc[i])
            except ValueError as ve:
                df[colname] = df[colname].drop(i, 0)
        df = df.dropna(axis=0)
        df[colname] = df[colname].astype(np.float64)
        #logging.error("to_float64() Error: DataFrame: df[%s], Error info: %s", colname, err)
        #df = df.drop(columns=colname)
    return df

def to_int(df, colname):
    try:
        if df.dtypes[colname] != np.int64:
            df[colname] = df[colname].astype(np.int64)
    except Exception as err:
        for i in range(1, len(df[colname])+1):
            try:
                np.int64(df[colname].loc[i])
            except ValueError as ve:
                df[colname] = df[colname].drop(i, 0)
        df = df.dropna(axis=0)
        df[colname] = df[colname].astype(np.int64)
        #logging.error("to_int() Error: DataFrame: df[%s], Error info: %s", colname, err)
        #df = df.drop(columns=colname)
    return df

def to_string(df, colname):
    try:
        if df.dtypes[colname] != object:
            df[colname] = df[colname].astype(str)
    except Exception as err:
        for i in range(1, len(df[colname])+1):
            try:
                str(df[colname].loc[i])
            except ValueError as ve:
                df[colname] = df[colname].drop(i, 0)
        df = df.dropna(axis=0)
        df[colname] = df[colname].astype(str)
        #logging.error("to_string() Error: DataFrame: df[%s], Error info: %s", colname, err)
        #df = df.drop(columns=colname)
    return df

if __name__ == '__main__':
    logging.basicConfig(filename='collecting_koroad.log', level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    reload(sys)
    sys.setdefaultencoding('utf-8')
    
    # Download the csv koroad files
    file_dir = os.getcwd() + '/koroad_files/'
    if not os.path.isdir(file_dir):
        os.mkdir(file_dir)
    for k, v in DOWNLOAD_URLS.items():
        fn = file_dir + k + '.csv'
        if not os.path.exists(fn):
            urllib.urlretrieve(v, fn)

    # Connect MongoDB
    client = mongodb_connect(id='', pw='', ip='', port='')
    db = client.public # Database name: public

    col = db.KOROAD # Collection name: KOROAD (도로교통공단)
    for f in os.listdir(file_dir):
        if f == 'death.csv':
            df = pd.read_csv(file_dir+f, encoding='CP949')
            df.columns = list(INFO_NEW_COLUMNS)
            df = df.drop(columns=["YEAR", "MINUTES"])
            df = to_string(df, 'DATETIME')
            df = to_float64(df, 'GPS_LONG')
            df = to_float64(df, 'GPS_LAT')
            df = to_int(df, 'NUM_CASUALTY')
            df = to_int(df, 'NUM_SEVERE')
            df = to_int(df, 'NUM_SLIGHTLY')
            df = to_int(df, 'NUM_INJURY')
            df = to_int(df, 'NUM_DEATH')
            kor_fn = mapping(f)
            df["DATANAME"] = kor_fn
            df_json = json.loads(df.to_json(orient='records'))
            col.insert(df_json)
            logging.info("file name %s uploaded ...", f)
        else:
            try:
                # Check if error_bad_lines exist
                df = pd.read_csv(file_dir+f, encoding='CP949')
                df.columns = list(ACCI_NEW_COLUMNS)
                df = df.drop(columns=["ID", "DISTRICT_CODE", "SPOT_CODE", "POLYGON"])
                df = to_string(df, 'DATE')
                df = to_float64(df, 'GPS_LONG')
                df = to_float64(df, 'GPS_LAT')
                df = to_int(df, 'NUM_OCCUR')
                df = to_int(df, 'NUM_CASUALTY')
                df = to_int(df, 'NUM_SEVERE')
                df = to_int(df, 'NUM_SLIGHTLY')
                df = to_int(df, 'NUM_INJURY')
                df = to_int(df, 'NUM_DEATH')
                kor_fn = mapping(f)
                df["DATANAME"] = kor_fn
                df_json = json.loads(df.to_json(orient='records'))
                col.insert(df_json)
                logging.info("file name %s uploaded ...", f)
            except Exception as err:
                errortype = err.message.split('.')[0].strip()
                if errortype == 'Error tokenizing data':
                    df = pd.read_csv(file_dir+f, encoding='CP949', names=ACCI_NEW_COLUMNS)
                    df = df.drop(0,0)
                    df = df.drop(columns=["ID", "DISTRICT_CODE", "SPOT_CODE", "POLYGON"])
                    df = to_string(df, 'DATE')
                    df = to_float64(df, 'GPS_LONG')
                    df = to_float64(df, 'GPS_LAT')
                    df = to_int(df, 'NUM_OCCUR')
                    df = to_int(df, 'NUM_CASUALTY')
                    df = to_int(df, 'NUM_SEVERE')
                    df = to_int(df, 'NUM_SLIGHTLY')
                    df = to_int(df, 'NUM_INJURY')
                    df = to_int(df, 'NUM_DEATH')
                    kor_fn = mapping(f)
                    df["DATANAME"] = kor_fn
                    df_json = json.loads(df.to_json(orient='records'))
                    col.insert(df_json)
                    logging.info("file name %s uploaded ...", f)
    logging.info("All data files saved ...")

