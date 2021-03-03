#-*- coding: utf-8 -*-
"""
    Author: Changwoo Kim (KETI) / cwkim@keti.re.kr
    collecting_ts_files.py: Reqeust information related with kosta.or.kr/data.go.kr using Restful API
"""
from pymongo import MongoClient, errors
import sys, os, time, logging
import datetime, pprint
import urllib, io, csv, json
import pandas as pd
import zipfile
from googletrans import Translator

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

def filename(filename, filetype):
    if filetype == 'zip':
        fn = filename.split('.zip')[0]
    if filetype == 'csv':
        fn = filename.split('.csv')[0]
    return fn

if __name__ == '__main__':
    logging.basicConfig(filename='collecting_ts_files.log', level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    reload(sys)
    sys.setdefaultencoding('utf-8')

    file_dir = os.getcwd() + '/ts_files/'
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

    # Connect MongoDB
    client = mongodb_connect(id='', pw='', ip='', port='')
    db = client.public # Database name: public
    collection = db.TS # Collection name: TS (한국교통안전공단)

    # Collecting csv files only 
    csv_files = []
    for dirpath, dirnames, filenames in os.walk(file_dir):
        for filename in filenames:
            if filename.split('.')[-1] == 'csv':
                csv_files.append(os.path.join(dirpath, filename))

    # Changing column names to Korean pronunciation
    translator = Translator()
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file, encoding='CP949')
            tmp_cols = list()
            for col in df.columns:
                if translator.detect(col).lang == 'ko': # Korean
                    col_kor = translator.translate(col).extra_data.get('translation')[-1][-1]
                    tmp_cols.append(col_kor)     
                elif translator.detect(col).lang == 'en': # English
                    tmp_cols.append(col) 
                else:
                    tmp_cols.append(col)
                    logging.warn("we cannot figure out the column name (%s)", col)
            df.columns = tmp_cols
            df["DATANAME"] = csv_file.split(file_dir)[1].split('.csv')[0]
            df_json = json.loads(df.to_json(orient='records'))
            collection.insert(df_json)
            logging.info("file name %s uploaded ...", csv_file)
        except Exception as err:
            logging.error("Error %s of file name %s was found ...", err, csv_file)

    logging.info("All data files saved ...")
