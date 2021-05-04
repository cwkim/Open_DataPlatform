import influxdb
import numpy as np
import json
from pymongo import MongoClient

class DrivingInfo:
    
    def __init__(self, in_host, in_port, in_username, in_password, in_database, in_measurement):
        self.in_host = in_host
        self.in_port = in_port
        self.in_username = in_username
        self.in_password = in_password
        self.in_database = in_database
        self.in_measurement = in_measurement

        self.driving_list = []

    def connectinfluxdf(self):
        self.influx_client = influxdb.InfluxDBClient(host = self.in_host, 
                                                port = self.in_port, 
                                                username = self.in_username, 
                                                password = self.in_password, 
                                                database = self.in_database)

    def DataframeToDict(self, df):
        self.dict = df.to_dict(orient='dict')

    def convertTimeToEpoch(self, date_time):
        # date_time = "%s.%s.%s %s:%s:%s" %(_time[6:8], _time[4:6], _time[:4], _time[8:10], _time[10:12], _time[12:14])
        epoch = int(time.mktime(date_time.timetuple()))
        return epoch

    def calc_drive_time_and_num(self, df):
        time_interval = 600

        time_list = df['RECORD_TIME'].to_list()
        time_list = sorted(time_list)

        drive_count = 0
        drive_time = 0
        for i in range(len(time_list)):
            if i == 0:
                start_time = self.convertTimeToEpoch(time_list[i])
            elif i == len(time_list) - 1:
                current_time = self.convertTimeToEpoch(time_list[i])
                previous_time = self.convertTimeToEpoch(time_list[i-1])
                drive_time += current_time - start_time + 1
                drive_count += 1
            else:
                current_time = self.convertTimeToEpoch(time_list[i])
                previous_time = self.convertTimeToEpoch(time_list[i-1])
                
                if current_time - previous_time > time_interval:
                    drive_time += previous_time - start_time + 1
                    drive_count += 1
                    start_time = current_time

        return drive_time, drive_count
        
    def calc_idling(self):
        time_ = []
        t_count = 0 
        for i in range(1, len(self.dict["RECORD_TIME"])-1):
            if self.dict["DRIVE_STATE"][i] == "S":
                
                if self.dict["DRIVE_STATE"][i-1] == self.dict["DRIVE_STATE"][i]:
                    t_count +=1
                    
                    if self.dict["DRIVE_STATE"][i] != self.dict["DRIVE_STATE"][i+1]:
                        time_.append(t_count)
                        t_count = 0 

        i_time = [i for i in time_ if i > 300 ]

        self.idling_time = sum(i_time)
        self.idling_count = len(i_time)

    def get_driving_info(self, df):
        self.driving_list = []
        self.record_time = self.dict["RECORD_TIME"][0].strftime("%Y-%m-%dT00:00:00Z")
        
        if len(self.dict) > 0:
            self.distance = int(df["DRIVE_LENGTH_DAY"].max())
            self.fuel = int(df["FUEL_CONSUM_DAY"].max())
            drive_time, drive_count = self.calc_drive_time_and_num(df)
            self.drive_time = drive_time
            self.drive_count = drive_count

            try:
                self.fuel_efficiency = round((self.distance / self.fuel), 2)
            except ZeroDivisionError as e:
                self.fuel_efficiency = float(0)

            self.calc_idling()
        else:
            self.distance = 0
            self.fuel = 0
            self.fuel_efficiency = float(0)
            self.idling_time = 0
            self.idling_count = 0
            self.drive_time = 0
            self.drive_count = 0

        self.driving_list.append([self.record_time, self.drive_time, self.drive_count, self.distance, self.fuel,
                                  self.fuel_efficiency, self.idling_time, self.idling_count])

    def savedrivinglist(self):
        self.connectinfluxdf()
        self.in_measurement = "DRIVING_INFO"

        car_id = self.dict['car_id'][0]
        for m in range(len(self.driving_list)):
            try:
                json_body = [
                    {
                        "measurement" : self.in_measurement,
                        "tags" : {
                            "car_id" : car_id
                        },
                        "time" : self.driving_list[m][0],
                        "fields" : {
                            "DRIVE_TIME" : self.driving_list[m][1],
                            "DRIVE_COUNT": self.driving_list[m][2],
                            "DISTANCE" : self.driving_list[m][3],
                            "FUEL" : self.driving_list[m][4],
                            "FUEL_EFFICIENCY" : self.driving_list[m][5],
                            "IDLING_TIME" : self.driving_list[m][6],
                            "IDLING_COUNT" : self.driving_list[m][7]
                        }
                    }
                ]     
                self.influx_client.write_points(json_body)

            except:
                continue
        print("Completed data input to influxDB.")
