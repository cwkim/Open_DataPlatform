# -*- coding:utf-8 -*-
import pandas as pd
import influxdb
from pymongo import MongoClient
from datatime import datetime
import json

class GetDataSet:
    def __init__(self, mo_address, mo_username, mo_password, mo_authsource, mo_database, mo_collection,
                in_host, in_port, in_username, in_password, in_database):
        self.mo_address = mo_address
        self.mo_username = mo_username
        self.mo_password = mo_password
        self.mo_authsource = mo_authsource
        self.mo_database = mo_database
        self.mo_collection = mo_collection
        
        self.in_host = in_host
        self.in_port = in_port
        self.in_username = in_username
        self.in_password = in_password
        self.in_database = in_database
        
    def connectinfluxdf(self):
        self.influx_client = influxdb.DataFrameClient(host = self.in_host, 
                                                    port = self.in_port, 
                                                    username = self.in_username, 
                                                    password = self.in_password, 
                                                    database = self.in_database)

    def connectmongodb(self):
        self.mo_client = MongoClient(self.mo_address,
                                username=self.mo_username,
                                password=self.mo_password,
                                authSource=self.mo_authsource,
                                connect=False)
        self.mo_db = self.mo_client.get_database(self.mo_database)
        self.mo_coll = self.mo_db.get_collection(self.mo_collection)
        print("MongoDB Connection complete")

    def loaddata(self, company_idx, car_id):
        self.mo_cursor = self.mo_coll.find({"$and":[{"COMPANY_IDX": company_idx}, {"VEHICLE_IDX":car_id}]},
                                            {"DRIVE_LENGTH_TOTAL":1, "FUEL_CONSUM_DAY":1, "DRIVE_LENGTH_DAY":1, "VEHICLE_IDX":1, "SEC_DATA":1, "RECORED_TIME":1, "_id":0}).sort([("_id", 1)])
        self.df = pd.DataFrame(list(self.mo_cursor))

    def check_cardata_count(self):

        show_coll_list = self.influx_client.get_list_measurements()
        coll_list = []

        for i in range(len(show_coll_list)):
            try:
                if show_coll_list[i]["name"][:12] == "DANGER_COUNT":
                    coll_list.append(show_coll_list[i]["name"])
            except:
                continue

        result_dict={}
        for coll in coll_list:                                 

            query_data = self.influx_client.query('SELECT * FROM "%s"' %coll)
        
            count = query_data[coll].groupby('car_id').size()
            count = count.head()
            count_dict = count.to_dict()
            
            result_dict[coll[12:]] = count_dict
        
        with open('count.json', 'w') as f:
            json.dump(result_dict, f)

    def StrtimeToDatetime(self):
        
        tmp = []
        for i in range(len(self.parsing_df)):
            convertTime = datetime.strptime(str(self.parsing_df["RECORD_TIME"].iloc[i]), "%Y%m%d%H%M%S")
            tmp.append(convertTime)
        del self.parsing_df["RECORD_TIME"]

        self.parsing_df["RECORD_TIME"] = tmp