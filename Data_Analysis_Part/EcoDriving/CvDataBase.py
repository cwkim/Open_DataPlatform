# -*- coding:utf-8 -*-
import influxdb
from datetime import datetime

class CvDataBase:
    def __init__(self, host, port, username, password, database):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        
        self.year, self.month, self.day = self.get_current_date()
        self.reshaped_danger_date = None

    def _connect_dataframe_db(self):
        ''' 데이터 DB(influxDB) 에 연결.
        params
        return
            influx_client: (client instance) dataFrame client danger driving data.
        '''
        influx_client = influxdb.DataFrameClient(self.host, self.port, self.username, 
                                                 self.password, self.database)
        return influx_client

    def get_current_date(self):

        ''' 현재 날짜 정보 반환.
        params
        return
            year: (string) year.
            month: (string) month - strict 2 digit.
            day: (string) day - strict 2 digit.
        '''
        
        now = datetime.now()
    
        year = str(now.year)
        month = str(now.month).zfill(2)
        day = str(now.day).zfill(2)
        
        return year, month, day