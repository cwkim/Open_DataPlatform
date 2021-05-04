import influxdb
import multiprocessing
from functools import partial
import folium
import pandas as pd
import CvDataBase

import AccidentClustoring



    '''
    1. 위험운전 쿼리
    2. MeanShift를 이용하여 위험운전지역 추출
    3. 임계치(5)를 설정하여 주요 위험지역 추출
    4. DB저장
    '''    
if __name__ == '__main__':
    influx_client = CvDataBase.CvDataBase(host='',
                                        port='',
                                        username='',
                                        password='',
                                        database='')
    
    influx_client = influx_client._connect_dataframe_db()
    
    carlist = [] # 차량 목록 입력
    radius = 0.0007 #반경
    date = "xxxx-xx-xx" #저장 날짜 입력
    final_df = pd.DataFrame(columns=[])

    for carid in carlist:
        print(carid)
        query_ = f"SELECT E_GPS_LAT, E_GPS_LONG, TYPE, car_id FROM {데이터셋 이름} WHERE \"car_id\" = '{carid}'"
        query_data = influx_client.query(query_)
        query_data = query_data["데이터셋 이름"]
        
        n_cluster, major_center = AccidentClustoring.MajorCenters(query_data, radius)

        pro_id = ['1', '2', '3', '4'] 
        pool = multiprocessing.Pool(processes=4)

        func = partial(AccidentClustoring.CenterMatching, query_data, major_center)
        m_result = pool.map(func, pro_id)
        
        pool.close()
        pool.join()
        
        result = m_result[0]
        for i in range(1, len(m_result)):
            result.extend(m_result[i])
        
        query_data["DANGER_POINT"] = result
        center_list = query_data["DANGER_POINT"].unique()
        
        # 임계치 적용하여 DANGER_POINT 추출한 뒤 DB에 저장    
        threshold = 5
        for center_point in center_list:
            query_df = query_data.query('DANGER_POINT == "%s"' %center_point)
            tmp_dict = {"GPS_LAT":{}, "GPS_LONG":{}, "car_id":{}}
            if len(query_df['DANGER_POINT']) > threshold:
                car_len = str(query_df["car_id"].unique()) # 확인작업
                print("car_list : %s" %car_len) #확인작업
                tmp_dict["GPS_LAT"][0] = major_center[center_point][0]
                tmp_dict["GPS_LONG"][0] = major_center[center_point][1]
                tmp_dict["car_id"][0] = query_df["car_id"].iloc[0]

                tmp_df = pd.DataFrame(tmp_dict)
                final_df = final_df.append(tmp_df)

    final_df = final_df.reset_index(drop=True)    
    final_df.index = pd.date_range(start=date, periods=len(final_df), freq='S')
    print(final_df)
    for car_id in carlist:
        car_df = final_df.query("car_id == '%s'" %car_id)
        del car_df["car_id"]
        influx_client.write_points(car_df, measurement="", tags={'car_id':car_id}, batch_size=1000)

###########################################################################################################################################        
    '''
        Folium 확인
    '''
        threshold = 30
        for _cen in center_list:
            query_df = query_data.query('DANGER_POINT == "%s"' %_cen)

            if len(query_df['DANGER_POINT']) > threshold:
                final_df = final_df.append(query_df)
    
    final_df = final_df.reset_index(drop=True)

    # Folium을 이용한 시각화
    center_p = [major_center[0][0], major_center[0][1]]
    m = folium.Map(location=center_p, zoom_start=13)
    folium.Marker(center_p, popup="Center").add_to(m)

    for j in range(len(final_df)):
        _point = [final_df["E_GPS_LAT"][j], final_df["E_GPS_LONG"][j]]
        folium.Circle(radius=25, location=_point, color='RED', fill=False).add_to(m)

    for i in final_df["DANGER_POINT"].unique():
        folium.Circle(location=[major_center[i][0], major_center[i][1]], radius=25, color='#ffffgg', fill_color='#fffggg', popup=str(j)).add_to(m)
    m.save("result.html")

 