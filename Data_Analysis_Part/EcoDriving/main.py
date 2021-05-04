# -*- coding:utf-8 -*-

import influxdb
import pandas as pd
from datetime import timedelta
from EcoDrive import EcoDrive
import CvDataBase

if __name__ == '__main__':

    '''
    일별 처리가 아니라 전체 데이터셋을 일괄적으로 처리
    1. 데이터 Load
    2. 공회전에 따른 연료 사용량 계산
    3. 최근 7일간의 연비 계산
    4. InfluxDB에 데이터 저장
    '''

    # 1.Dataset 준비단계
    influx_client = CvDataBase.CvDataBase(host="",
                                         port='',
                                        username='',
                                        password='',
                                        database='')

    influx_client = influx_client._connect_dataframe_db()
    query_ = f"SELECT * FROM {데이터셋 이름}}"
    query_data = influx_client.query(query_)
    query_data = query_data["데이터셋 이름"]
    
    # 2.공회전에 따른 연료 사용량 계산 단계
    eco = EcoDrive()
    df = eco.CalcFuelconsumForIdling(query_data)
    
    # 3.기준일로부터 최근 7일간의 연비평균와 최근 7일간의 누적 공회전에 따른 연료 사용량 
    # ex) 기준일이 6/15일이면, 6/8~14일까지의 평균연비가 6/15 AVER_FE 컬럼에 입력
    
    date_list = pd.date_range() #날짜 입력
    car_list = [] #차량 목록 입력

    for carid in car_list:
        query_df = df.query("car_id == '%s'" %carid)
        reset_index_df = query_df.reset_index()

        for date in date_list:
            sel = reset_index_df.query("index == '%s'" %date)
        
            if len(sel) != 0:
                before_date = date - timedelta(days=7)
                second_query_df = reset_index_df[(reset_index_df['index'] >= str(before_date)) & (reset_index_df['index'] < str(date))]
                aver_fe = eco.RecentAverageFE(second_query_df, date)
                sum_idl_fuel = eco.RecentSumIDL_FUEL(second_query_df, date)
                best_fe = eco.RecentBestFE(second_query_df, 30, date)

                query_df.loc[date, "AVER_FE"] = aver_fe
                query_df.loc[date, "SUM_IDL_FUEL"] = sum_idl_fuel
                query_df.loc[date, "BEST_AVER_FE"] = best_fe
            else:
                pass
        print(query_df)
        del query_df["car_id"]
        influx_client.write_points(query_df, measurement="", tags={'car_id':carid}, batch_size=1000)