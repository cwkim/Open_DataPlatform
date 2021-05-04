# -*- coding:utf-8 -*-
import pandas as pd
import pymongo
import influxdb
from DangerService import DangerService
import CvDataBase

   '''
    1. 위험운전 횟수 쿼리
    2. 위험운전에 가중치 적용
    3. Rank, Score, Percent, Grade 계산
    4. 위험운전의 상태 표시 (양호, 주의, 경고, 위험)
    5. DB에 데이터 저장
    '''

if __name__ == '__main__':

    influx_client = CvDataBase.CvDataBase(host='',
                                        port='',
                                        username='',
                                        password='',
                                        database='')
    DrivingScore = DangerService()

    influx_client = influx_client._connect_dataframe_db()
    date_list = pd.date_range() #날짜 입력

    for date in date_list:
        print(date)
        date = str(date)[:10]
        query_ = f"SELECT * FROM {데이터셋 이름} WHERE time = '{date}T00:00:00Z'"
        query_data = influx_client.query(query_)
        query_data = query_data[데이터셋 이름]
        
        col_list = ["A", "D", "Q", "S", "T", "U", "SL", "O", "car_id"]
        if len(query_data) == 0:
            continue
        else:
            weighted_data = DrivingScore.calc_weighted(query_data[col_list])
            
            weighted_data['RECORD_TIME'] = pd.to_datetime(str(date)+"T00:00:00Z")
            weighted_data = weighted_data.set_index(["RECORD_TIME"])
            
            score = DrivingScore.calc_totalscore(weighted_data)

            rank = DrivingScore.rank(query_data, score["TOTAL_SCORE"])
            state = DrivingScore.state(query_data)

            merge_outer = pd.merge(rank,state, how='outer',on='car_id')
            merge_outer['RECORD_TIME'] = pd.to_datetime(str(date)+"T00:00:00Z")
            merge_outer = merge_outer.set_index(["RECORD_TIME"])
            
            carlist = merge_outer["car_id"].unique()

            for car_id in carlist:
                car_df = merge_outer.query("car_id == '%s'" %car_id)
                del car_df["car_id"]
                influx_client.write_points(car_df, measurement="", tags={'car_id':car_id}, batch_size=1000)
