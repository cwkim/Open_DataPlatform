#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pymysql
import os
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
    parser.add_argument("-count", type=int, required=False, default=10, help="number of rows to query from table")
    
    args = parser.parse_args()
    
    return args


if __name__ == "__main__":

    _args_ = brush_argparse()

    MARIA_HOST = _args_.ip
    MARIA_PORT = _args_.port
    USER_NAME = _args_.user
    PASSWORD = _args_.pw
    DB_NAME = _args_.dbname
    TABLE_NAME = _args_.tablename
    QUERY_COUNT = _args_.count


    conn = pymysql.connect(host = MARIA_HOST, port = MARIA_PORT, user = USER_NAME, password = PASSWORD, db = DB_NAME, charset='utf8')   
    curs = conn.cursor()  
    
    sql = "select * from %s limit %s"%(TABLE_NAME, str(QUERY_COUNT))
    print("\n[EXECUTE] %s\n"%(sql))
    curs.execute(sql)
    result = curs.fetchall()
    with open("./out.txt", 'w') as f:
        for row in result:
            print(row)
            f.write(str(row)+'\n')
    
    conn.commit()
    conn.close()
