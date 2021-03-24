#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pymysql
import os
import pandas as pd
import sys
import time
import argparse


def brush_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ip", type=str, required=False, default='keti-ev.iptime.org', help="DB server ip")
    parser.add_argument("-port", type=int, required=False, default=3307, help="DB server port")
    parser.add_argument("-user", type=str, required=False, default='keti', help="DB server username")
    parser.add_argument("-pw", type=str, required=False, default='keti1234', help="DB server password")
    parser.add_argument("-dbname", type=str, required=False, default='testdb', help="target or want to make table name database name")
    parser.add_argument("-tablename", type=str, required=False, default='testtable', help="target or want to make table name table name")

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


if __name__ == "__main__":

    _args_ = brush_argparse()

    MARIA_HOST = _args_.ip
    MARIA_PORT = _args_.port
    USER_NAME = _args_.user
    PASSWORD = _args_.pw
    DB_NAME = _args_.dbname
    TABLE_NAME = _args_.tablename
    CSV_DIR = '../CSV_in'

    conn = pymysql.connect(host = MARIA_HOST, port = MARIA_PORT, user = USER_NAME, password = PASSWORD, charset='utf8')   
    curs = conn.cursor()  
    
    # SQL Statement to create a database
    sql = "CREATE DATABASE IF NOT EXISTS " + DB_NAME
    curs.execute(sql)

    conn.select_db(DB_NAME)
    curs = conn.cursor()
    
    file_list = []
    recursive_search_dir(CSV_DIR, file_list)

    for _filepath in file_list:
        print('\n' + _filepath + " 읽는 중..")
        try:
            # df1은 위에 두 줄 스킵, df2는 위에 두 줄만
            df = pd.read_csv(_filepath, low_memory=False, encoding="utf-8")
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(_filepath, low_memory=False, encoding="euc-kr")
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(_filepath, low_memory=False, encoding="cp949")
                except UnicodeDecodeError:
                    print("[WARN] + : " + _filepath)
                    print("Can't read")
                    continue
        
        if len(df) == 0:
            print("[WARN] + : " + _filepath)
            print("DataFrame is empty")
            continue
            
        # mysql NaN 인식 불가 (NaN -> None)
        df.where(df.notnull(), None)

        columns = list(df.columns)
        for col in columns:
            if col.startswith("Unnamed:"):
                del df[col]
        
        columns = list(df.columns)
        vals = df.values.tolist()
        dtypes = list(df.dtypes)

        ### CREATE TABLE 및 INSERT 문 string 생성 ###
        maketable_str = "("
        cols_str = "("
        vals_str = "("
        for i in range(len(columns)):
            cols_str = "%s%s%s"%(cols_str,columns[i]," ,")
            vals_str = "%s%s, "%(vals_str, "%s")
            if str(dtypes[i]).startswith("int"):
                maketable_str = "%s%s%s"%(maketable_str, columns[i], " INT, ")
            elif str(dtypes[i]).startswith("float"):
                maketable_str = "%s%s%s"%(maketable_str, columns[i], " FLOAT, ")
            else:
                maketable_str = "%s%s%s"%(maketable_str, columns[i], " VARCHAR(255), ")
        cols_str = "%s%s"%(cols_str[0:-2], ')')
        vals_str = "%s%s"%(vals_str[0:-2], ')')
        maketable_str = "%s%s"%(maketable_str[0:-2], ')')
        #############################################

        sql = "\nCREATE TABLE IF NOT EXISTS " + TABLE_NAME + maketable_str
        print('\n[Table 생성 SQL 문] ' + sql)
        curs.execute(sql)

        sql = "insert into " + TABLE_NAME + cols_str + " values " + vals_str
        print("\n[Insert DATA SQL 문] " + sql)
        curs.executemany(sql , vals)
        print(str(len(vals)) + " 데이터 전송 완료")
    
    conn.commit()
    conn.close()
