# -*- coding:utf-8 -*-
import json
import influxdb
from calculate_angle import get_angle
import pandas as pd

class DangerDriving:
    def __init__(self, in_host, in_port, in_username, in_password, in_database, in_measurement):
        self.in_host = in_host
        self.in_port = in_port
        self.in_username = in_username
        self.in_password = in_password
        self.in_database = in_database
        self.in_measurement = in_measurement

        self.base_info = self.get_base_info()
        self.driving_list = []
        self.df_status = False

    def connectinfluxdf(self):
        self.influx_client = influxdb.InfluxDBClient(host = self.in_host, 
                                                port = self.in_port, 
                                                username = self.in_username, 
                                                password = self.in_password, 
                                                database = self.in_database)

    def __gettimedifference(self, p1, p2):
        timediff = (p2 - p1).total_seconds()
        if (timediff < 0): timediff = -timediff
        return timediff

    def get_base_info(self):
        with open('danger_base.json', 'r') as base:
            result = json.load(base)
        return result
    
    def DataframeToDict(self, df):
        self.dict = df.to_dict(orient='dict')

    def reduce2start_end(self, df, sec):
        df = df.reset_index()
        copied_df = df.copy()


        for i in range(len(df)):
            if i == 0:
                before_time = df.loc[i]['RECORD_TIME']
                continue
            elif i+1 == len(df): break
            else:
                # print(i)
                current_time = df.loc[i]['RECORD_TIME']
                if (current_time - before_time).total_seconds() > sec :
                    before_time = df.loc[i]['RECORD_TIME']
                else:
                    before_time = df.loc[i]['RECORD_TIME']
                    next_time = df.loc[i+1]['RECORD_TIME']
                    if (next_time - current_time).total_seconds() < sec :
                        copied_df = copied_df.drop(index=i, axis=0)

        start_end_df = copied_df

        return start_end_df

    def __gettimedifference(self, p1, p2):
        timediff = (p2 - p1).total_seconds()
        if (timediff < 0): timediff = -timediff
        return timediff

    def getacceldecel(self, vehicle_type='truck'):
        """get accel or decel data
        
        params:
            vehicle_type:
                truck
                bus
                taxi
        """
        print("Accel, Decel, Quick Start and Sudden stop are being calculated.")
        # A = 'Accel', Q = 'Quick Start' , D = 'Decel', S = 'Sudden stop'
        self.danger_list = []
        i = 0
        j = 0
        
        outbool = False
        status = 'N'

        while i < len(self.dict['RECORD_TIME'])-1:
            point_s = (self.dict['DRIVE_SPEED'][i], self.dict['RECORD_TIME'][i])
            
            if j >= len(self.dict['RECORD_TIME']) - 1: break
            if i >= len(self.dict['RECORD_TIME']) - 2: break

            j = i + 1            

            while j <= len(self.dict['RECORD_TIME'])-1:

                point_e = (self.dict['DRIVE_SPEED'][j], self.dict['RECORD_TIME'][j])
                
                try:
                    # 시간차이 = 0 -> 분모 = 0 -> error 발생
                    diff = (point_e[0] - point_s[0]) / self.__gettimedifference(point_e[1], point_s[1])
                except:
                    j+=1
                    continue

                """위험운행 조건 시작"""
                # 급가속
                if(diff > self.base_info['accel'][vehicle_type]['diff'] and point_s[0] > self.base_info['accel'][vehicle_type]['start']):
                    status = 'A'
                
                # 급출발 
                elif(diff > self.base_info['quick_start'][vehicle_type]['diff'] and point_s[0] < self.base_info['quick_start'][vehicle_type]['start']):
                    status = 'Q'
                    
                # 급감속
                elif(diff < self.base_info['deacc'][vehicle_type]['diff'] and point_e[0] > self.base_info['deacc'][vehicle_type]['end']):
                    status = 'D'
                    
                # 급정지
                elif(diff < self.base_info['sudden_stop'][vehicle_type]['diff'] and point_e[0] < self.base_info['sudden_stop'][vehicle_type]['end']):
                    status = 'S'
                    
                else:
                    outbool = True
                    
                    
                if outbool == True:
                    if status != 'N':
                        """저장"""
                        self.danger_list.append([i, j-1, status, None])
                    """초기화"""
                    status = 'N'
                    i = j
                    outbool = False
                    break

                j+=1

    def get_turn_data(self, df, vehicle_type='truck'):
        
        # step1. 초기 설정
        base_info = self.get_base_info()
        self.danger_list = []
        dt = 4
        ref_angle = base_info['turn']['truck']['cumulated'].split(',')
        angle_lower = int(ref_angle[0])
        angle_upper = int(ref_angle[1])
        
        count = 0
        new_dict = {'S_TIME':{}, 'E_TIME':{}, 'S_SPEED':{}, 'E_SPEED':{}, 'CUMULATED_ANGLE':{}}

        #급좌우회전 조건1 : 속도 20 이상일 때
        query_df = df.query("DRIVE_SPEED > 19").reset_index(drop=True)
        self.DataframeToDict(query_df)

        if len(query_df) < 10:
            self.df_status = False
            return self.df_status
        else:
            self.df_status = True
        
        
        #급좌우회전 조건2 : 4초 이내에 120~160도 사이로 변하는 구간 데이터 기록(중복 허용)
        for i in range(1, len(query_df['RECORD_TIME'])-dt):
            
            real_dt = self.__gettimedifference(self.dict['RECORD_TIME'][i+dt], self.dict['RECORD_TIME'][i])
            is_switch = True
            
            if real_dt > dt:
                continue
            
            else:
                diff_angle_list = []
                for j in range(dt):
                    diff_angle = abs(self.dict['GPS_ANGLE'][i+j+1] - self.dict['GPS_ANGLE'][i+j])
                    diff_angle = 360 - diff_angle if diff_angle > 180 else diff_angle
                    diff_angle_list.append(diff_angle)
                
                cumulated_angle = sum(diff_angle_list)
                
                if cumulated_angle < angle_lower or cumulated_angle > angle_upper:
                    is_switch = False
            
                if(is_switch):
                    new_dict["S_TIME"][count] = i
                    new_dict["E_TIME"][count] = i+dt
                    new_dict["S_SPEED"][count] = self.dict['DRIVE_SPEED'][i+j]
                    new_dict["E_SPEED"][count] = self.dict['DRIVE_SPEED'][i+j+1]
                    new_dict["CUMULATED_ANGLE"][count] = cumulated_angle
                    count += 1
        
        new_df = pd.DataFrame(new_dict)

        #급좌우회전 조건2-1 : 조건2에 걸린 데이터와 원본데이터 매칭
        tmp_df = pd.DataFrame(columns=[])
        for i in range(len(new_df)):
            for j in range(new_df['S_TIME'][i], new_df['E_TIME'][i]+1):
                tmp_df = tmp_df.append(query_df.iloc[j])

        tmp_df = tmp_df.drop_duplicates(["RECORD_TIME"])

        #급좌우회전 조건3 : 중복 제거 10초안에 좌우회전이 발생시 하나의 좌우회전으로 간주
        rm_result = self.reduce2start_end(tmp_df, 10)
        rm_result = rm_result.reset_index(drop=True)
        
        try:
            start_df = rm_result[['RECORD_TIME', 'index']].iloc[::2].reset_index(drop=True)
            end_df = rm_result[['RECORD_TIME', 'index']].iloc[1::2].reset_index(drop=True)


            for i in range(len(start_df)):
                diff_angle_list = []
                for j in range(start_df.loc[i, 'index'], end_df.loc[i, 'index']+1):
                    diff_angle = abs(self.dict['GPS_ANGLE'][j+1] - self.dict['GPS_ANGLE'][j])
                    diff_angle = 360 - diff_angle if diff_angle > 180 else diff_angle
                    diff_angle_list.append(diff_angle)
                
                #급좌우회전 조건 4
                #중복 데이터를 제외하다보니 급좌우회전 조건의 시간보다 길어짐
                #따라서 한번 더 체크하여 U 조건에 걸리면 T -> U 변경 (but. angle 조건을 기존보다 좀더 러프하게 줌)
                if (end_df.loc[i, 'index'] - start_df.loc[i, 'index']) < 8 and (sum(diff_angle_list) > 150):
                    d_type = "U"
                else:
                    d_type = "T"
                    
                self.danger_list.append([start_df.loc[i, 'index'], end_df.loc[i, 'index'], d_type, sum(diff_angle_list)])
        except:
            pass

    def get_uturn_data(self, df, vehicle_type='truck'):
        
        # step1. 초기 설정
        base_info = self.get_base_info()
        self.danger_list = []
        dt = 8
        ref_angle = base_info['u_turn']['truck']['cumulated'].split(',')
        angle_lower = int(ref_angle[0])
        angle_upper = int(ref_angle[1])
        
        count = 0
        new_dict = {'S_TIME':{}, 'E_TIME':{}, 'S_SPEED':{}, 'E_SPEED':{}, 'CUMULATED_ANGLE':{}}

        #급유턴 조건1 : 속도 15 이상일 때
        query_df = df.query("DRIVE_SPEED > 14").reset_index(drop=True)
        self.DataframeToDict(query_df)

        if len(query_df) < 10:
            self.df_status = False
            return self.df_status
        else:
            self.df_status = True
        
        
        #급유턴 조건2 : 8초 이내에 160~180도 사이로 변하는 구간 데이터 기록(중복 허용)
        for i in range(1, len(query_df['RECORD_TIME'])-dt):
            
            real_dt = self.__gettimedifference(self.dict['RECORD_TIME'][i+dt], self.dict['RECORD_TIME'][i])
            is_switch = True
            
            if real_dt > dt:
                continue
            
            else:
                diff_angle_list = []
                for j in range(dt):
                    diff_angle = abs(self.dict['GPS_ANGLE'][i+j+1] - self.dict['GPS_ANGLE'][i+j])
                    diff_angle = 360 - diff_angle if diff_angle > 180 else diff_angle
                    diff_angle_list.append(diff_angle)
                
                cumulated_angle = sum(diff_angle_list)
                
                if cumulated_angle < angle_lower or cumulated_angle > angle_upper:
                    is_switch = False
            
                if(is_switch):
                    new_dict["S_TIME"][count] = i
                    new_dict["E_TIME"][count] = i+dt
                    new_dict["S_SPEED"][count] = self.dict['DRIVE_SPEED'][i+j]
                    new_dict["E_SPEED"][count] = self.dict['DRIVE_SPEED'][i+j+1]
                    new_dict["CUMULATED_ANGLE"][count] = cumulated_angle
                    count += 1
        
        new_df = pd.DataFrame(new_dict)

        #급유턴 조건2-1 : 조건2에 걸린 데이터와 원본데이터 매칭
        tmp_df = pd.DataFrame(columns=[])
        for i in range(len(new_df)):
            for j in range(new_df['S_TIME'][i], new_df['E_TIME'][i]+1):
                tmp_df = tmp_df.append(query_df.iloc[j])

        tmp_df = tmp_df.drop_duplicates(["RECORD_TIME"])

        #급유턴 조건3 : 중복 제거 10초안에 유턴이 발생시 하나의 유턴으로 간주
        rm_result = self.reduce2start_end(tmp_df, 10)
        rm_result = rm_result.reset_index(drop=True)
        
        try:
            start_df = rm_result[['RECORD_TIME', 'index']].iloc[::2].reset_index(drop=True)
            end_df = rm_result[['RECORD_TIME', 'index']].iloc[1::2].reset_index(drop=True)


            for i in range(len(start_df)):
                diff_angle_list = []
                for j in range(start_df.loc[i, 'index'], end_df.loc[i, 'index']+1):
                    diff_angle = abs(self.dict['GPS_ANGLE'][j+1] - self.dict['GPS_ANGLE'][j])
                    diff_angle = 360 - diff_angle if diff_angle > 180 else diff_angle
                    diff_angle_list.append(diff_angle)
                    
                self.danger_list.append([start_df.loc[i, 'index'], end_df.loc[i, 'index'], "U", sum(diff_angle_list)])
        except:
            pass

    def get_lane_switch_data(self, df, vehicle_type='truck'):
        '''get sudden lane switch or outrun data
        
        params:
            vehicle_type:
                truck
                bus
                taxi
        '''
        #### 급진로변경/급앞지르기 공통 구간 알고리즘 ####
        # step1. 초기 설정
        base_info = self.get_base_info()
        self.danger_list = []
        dt = 5
        ref_angle = base_info['lane_switch']['truck']['angle']
        base_angle = base_info['lane_switch']['truck']['angle_per_sec']
        base_speed = base_info['lane_switch']['truck']['s']
        base_speed_switch = base_info['lane_switch']['truck']['switch_accel'] # 2~3 사이 조건때문에 사용 안함
        base_speed_outrun = base_info['lane_switch']['truck']['outrun_accel']
        
        count = 0

        new_dict = {'S_TIME':{}, 'E_TIME':{}, 'S_SPEED':{}, 'E_SPEED':{}, 'DIFF_ANGLE':{}}

        #급차로변경 조건1 : 속도 30 이상 (20이상으로 한 이유는 급차로 변경의 감속조건 때문)
        
        query_df = df.query("DRIVE_SPEED > 20").reset_index(drop=True)
        self.DataframeToDict(query_df)

        if len(query_df) < 10:
            self.df_status = False
            return self.df_status
        else:
            self.df_status = True

    
        #급차로변경 조건2 : 좌우측으로 초당 6도 이상으로 변화하는 조건(중복 허용) / 7초 동안 (중간에 데이터포인트 안찍히는거 고려함)
        for i in range(1, len(query_df['RECORD_TIME'])-dt):
            
            real_dt = self.__gettimedifference(self.dict['RECORD_TIME'][i+dt], self.dict['RECORD_TIME'][i])
            is_switch = True
            
            if real_dt > dt+2:
                continue
            elif self.dict["DRIVE_SPEED"][i] < base_speed:
                continue
            
            else:
                diff_angle_list = []
                for j in range(dt):
                    diff_angle = self.dict['GPS_ANGLE'][i+j+1] - self.dict['GPS_ANGLE'][i+j]
                    
                    if abs(diff_angle) < ref_angle:
                        is_switch = False
                        break
                    elif abs(diff_angle) >= ref_angle:
                        diff_angle_list.append(round(diff_angle, 3))
                        continue    
            
                if(is_switch):
                    new_dict["S_TIME"][count] = i
                    new_dict["E_TIME"][count] = i+dt
                    new_dict["S_SPEED"][count] = self.dict['DRIVE_SPEED'][i+j]
                    new_dict["E_SPEED"][count] = self.dict['DRIVE_SPEED'][i+j+1]
                    new_dict["DIFF_ANGLE"][count] = abs(sum(diff_angle_list)/len(diff_angle_list))
                    count += 1


        #급차로변경 조건3 : 5초동안 누적각도 2 초과하는 경우 제거
        new_df = pd.DataFrame(new_dict)
        query_ = f"DIFF_ANGLE <= {base_angle}"
        new_df = new_df.query(query_).reset_index(drop=True)
        
        tmp_df = pd.DataFrame(columns=[])
    

        for i in range(len(new_df)):
            for j in range(new_df['S_TIME'][i], new_df['E_TIME'][i]+1):
                tmp_df = tmp_df.append(query_df.iloc[j])

        tmp_df = tmp_df.drop_duplicates(["RECORD_TIME"])
        

        #급차로변경 조건4 및 중복제거 : 초당 가감속 ±2 이하인 경우 -> 급진로변경  / 가속이 초당 3km 이상인 경우 급앞지르기
        rm_result = self.reduce2start_end(tmp_df, 10)
        rm_result = rm_result.reset_index(drop=True)

        try:
            start_df = rm_result[['RECORD_TIME', 'index']].iloc[::2].reset_index(drop=True)
            end_df = rm_result[['RECORD_TIME', 'index']].iloc[1::2].reset_index(drop=True)
            
            for i in range(len(start_df)):
                status = "N"
                diff_list = []
                for j in range(start_df.loc[i, 'index'], end_df.loc[i, 'index']):
                    diff_list.append(df.loc[j+1, "DRIVE_SPEED"]-df.loc[j, "DRIVE_SPEED"])

                avr_spd = (sum(diff_list) / (end_df.loc[i, 'index'] - start_df.loc[i, 'index']))
                
                #base_speed_switch인 경우에 2~3 사이의 값이 나오면 N 값을 갖음 따라서 자체적으로 급앞지르기의 조건을 기준으로 판단
                if avr_spd < base_speed_outrun: 
                    status = "SL"
                elif avr_spd >= base_speed_outrun:
                    status = "O"
                
                self.danger_list.append([start_df.loc[i, 'index'], end_df.loc[i, 'index'], status, None])
        except:
            pass

    def savedangerlist(self):
        """save danger list into influxdb"""
        self.connectinfluxdf()
        
        if self.df_status == True:
            car_id = self.dict['car_id'][0]
            for m in range(len(self.danger_list)):
                i = self.danger_list[m][0]          
                j = self.danger_list[m][1]
                
                try:
                    json_body = [
                        {
                            "measurement" : self.in_measurement,
                            "tags": {
                                "car_id": car_id
                            },
                            "time": self.dict['RECORD_TIME'][i].strftime('%Y-%m-%dT%H:%M:%SZ'),
                            "fields": {
                                "S_TIME": self.dict['RECORD_TIME'][i].strftime('%Y-%m-%dT%H:%M:%SZ'),
                                "E_TIME": self.dict['RECORD_TIME'][j].strftime('%Y-%m-%dT%H:%M:%SZ'),
                                "TIME_DIFF": int(self.__gettimedifference(self.dict['RECORD_TIME'][i], self.dict['RECORD_TIME'][j])),
                                "S_DRIVE_SPEED": int(self.dict['DRIVE_SPEED'][i]),
                                "E_DRIVE_SPEED": int(self.dict['DRIVE_SPEED'][j]),
                                "SPEED_DIFF" : int((self.dict['DRIVE_SPEED'][j] - self.dict['DRIVE_SPEED'][i])),
                                "S_GPS_LAT" : self.dict['GPS_LAT'][i],
                                "S_GPS_LONG" : self.dict['GPS_LONG'][i],
                                "E_GPS_LAT" : self.dict['GPS_LAT'][j],
                                "E_GPS_LONG" : self.dict['GPS_LONG'][j],
                                "STACKED_ANGLE" : self.danger_list[m][3],
                                "TYPE" : self.danger_list[m][2]
                            }
                        }
                    ]
                    self.influx_client.write_points(json_body)
                    print(json_body)
                    
                except:
                    continue
            print("Completed data input to influxDB.")
        else:
            pass
            
