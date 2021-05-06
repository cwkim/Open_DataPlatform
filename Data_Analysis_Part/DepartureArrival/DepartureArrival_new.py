import pandas as pd
import time
from datetime import datetime, timedelta
import influxdb
from haversine import haversine
import sys


def DepartureArrival():
    #DB 접속정보
    client = influxdb.DataFrameClient()

    #차량별 최신 위치 데이터 쿼리
    data = dict(client.query("SELECT GPS_LAT, GPS_LONG, car_id, time FROM MAIN2 GROUP BY car_id ORDER BY time DESC LIMIT 1"))
    new_location = dict()

    for val in data:
        temp_df = data[val]
        temp_df = Factory_Geofencing(temp_df)
        temp_date = (temp_df.index.to_list()[0]).to_pydatetime()
        new_location[temp_df['car_id'][0]] = {'GPS_LAT': temp_df['GPS_LAT'][0], 'GPS_LONG': temp_df['GPS_LONG'][0], 'LOC_NUM': temp_df['LOC_NUM'][0], 'LOC_NAME': temp_df['LOC_NAME'][0], 'time': temp_date.strftime('%Y-%m-%dT%H:%M:%SZ')}


    #차량별 최근 출도착 데이터 쿼리
    data_before = client.query("SELECT GPS_LAT, GPS_LONG, LOC_NUM, LOC_NAME, CAR_ID FROM DEPARTUREARRIVAL GROUP BY car_id ORDER BY time DESC LIMIT 1")
    old_location = dict()

    for val in data_before:
        temp_dict = data_before[val]
        temp_date = (temp_dict.index.to_list()[0]).to_pydatetime()
        temp_dict = temp_dict.reset_index()
        old_location[temp_dict['CAR_ID'][0]] = {'time':temp_date, 'GPS_LAT':temp_dict['GPS_LAT'][0], 'GPS_LONG':temp_dict['GPS_LONG'][0], 'LOC_NUM': temp_dict['LOC_NUM'][0], 'LOC_NAME': temp_dict['LOC_NAME'][0]}

    #new location이 항상 수가 더 많음
    print(len(new_location.keys()))
    print(len(old_location.keys()))

    print(new_location.keys())
    print(old_location.keys())

    client = influxdb.InfluxDBClient()

    for keys in new_location:
        try:
            if(new_location[keys]['LOC_NUM'] != old_location[keys]['LOC_NUM']):
                print(old_location[keys]['LOC_NUM'])
                print(new_location[keys]['LOC_NUM'])
                if new_location[keys]['LOC_NUM'] == 0: base_num = old_location[keys]['LOC_NUM']
                else : base_num = new_location[keys]['LOC_NUM']
                #새로운친구 저장
                json_body = [
                    {
                        "measurement": "DEPARTUREARRIVAL",
                         "tags": {
                             "car_id": keys
                         },
                         "time": new_location[keys]['time'],
                        "fields": {
                            "CAR_ID": keys,
                            "GPS_LAT": new_location[keys]['GPS_LAT'],
                            "GPS_LONG": new_location[keys]['GPS_LONG'],
                            "LOC_NAME": new_location[keys]['LOC_NAME'],
                            "LOC_NUM": int(new_location[keys]['LOC_NUM']),
                            "BASE_NUM": int(base_num)
                            }
                        }
                    ]
                print(json_body)
                client.write_points(json_body)
        except:
            base_num = new_location[keys]['LOC_NUM']
            json_body = [
                {
                    "measurement": "DEPARTUREARRIVAL",
                    "tags": {
                        "car_id": keys
                    },
                    "time": new_location[keys]['time'],
                    "fields": {
                        "CAR_ID": keys,
                        "GPS_LAT": new_location[keys]['GPS_LAT'],
                        "GPS_LONG": new_location[keys]['GPS_LONG'],
                        "LOC_NAME": new_location[keys]['LOC_NAME'],
                        "LOC_NUM": int(new_location[keys]['LOC_NUM']),
                        "BASE_NUM": int(base_num)
                    }
                }
            ]
            print(json_body)
            client.write_points(json_body)


#향후 Factory_Geofencing 발전계획
# 1) 최근접 쌍찾기(합병정렬 추가) 방법론을 이용하여 최적화
# 2) 공장별 격자형 위치 만들어보기
def Factory_Geofencing(gps):
    ######################## 삼표 공장 GPS 좌표 ########################
    factory_loc = {
        1: {'lat': 37.545130, 'long': 127.035287, 'name': 'SeongSu'},
        2: {'lat': 37.532909, 'long': 127.111099, 'name': 'PungNap'},
        3: {'lat': 37.408363, 'long': 127.207935, 'name': 'Gwangju'},
        4: {'lat': 37.636615, 'long': 127.155069, 'name': 'DongSeoul'},
        5: {'lat': 37.818462, 'long': 126.988775, 'name': 'YangJu'},
        6: {'lat': 37.996115, 'long': 127.101713, 'name': 'YeonCheon'},
        7: {'lat': 37.616722, 'long': 126.861966, 'name': 'SeoBu'},
        8: {'lat': 37.755099, 'long': 126.871794, 'name': 'IlSan'},
        9: {'lat': 37.714737, 'long': 126.576829, 'name': 'Gimpo'},
        10: {'lat': 37.384424, 'long': 126.93916, 'name': 'AnYang'},
        11: {'lat': 37.219052, 'long': 126.866936, 'name': 'HwaSeong'},
        12: {'lat': 37.485207, 'long': 126.670239, 'name': 'InCheon'},
        13: {'lat': 37.399269, 'long': 126.721126, 'name': 'SongDo'},
        14: {'lat': 37.028811, 'long': 127.019815, 'name': 'PyeongTaek'},
        15: {'lat': 37.106637, 'long': 126.979138, 'name': 'OSan'},
        16: {'lat': 37.296330, 'long': 127.600196, 'name': 'YeoJu'},
        17: {'lat': 37.351109, 'long': 127.8724, 'name': 'WonJu'},
        18: {'lat': 37.012386, 'long': 127.762797, 'name': 'ChungJu'},
        19: {'lat': 37.194688, 'long': 127.193213, 'name': 'YongIn'},
        20: {'lat': 37.070250, 'long': 127.202649, 'name': 'AnSeong'},
        21: {'lat': 36.856278, 'long': 127.017596, 'name': 'ASan'},
        22: {'lat': 36.886259, 'long': 126.786646, 'name': 'DangJin'},
        23: {'lat': 36.573085, 'long': 127.380382, 'name': 'CheongWon'},
        24: {'lat': 36.372246, 'long': 127.423035, 'name': 'DaeJeon'},
        25: {'lat': 36.919185, 'long': 127.241760, 'name': 'NamBu'},
        26: {'lat': 35.032409, 'long': 126.739419, 'name': 'NamGwangJu'},
        27: {'lat': 35.069101, 'long': 128.990734, 'name': 'SeoBusan'},
        28: {'lat': 36.12241, 'long': 129.343453, 'name': 'PoHang'},
        29: {'lat': 35.966672, 'long': 126.623523, 'name': 'GunSan'}
    }

    for i in range(len(factory_loc)):
        dist = get_haversine_distance(gps['GPS_LAT'], gps['GPS_LONG'], factory_loc[i+1]['lat'], factory_loc[i+1]['long'])
        if(dist <= 200):
            gps['LOC_NUM'] = i+1
            gps['LOC_NAME'] = factory_loc[i+1]['name']
            print(gps)
            return gps
    gps['LOC_NUM'] = 0
    gps['LOC_NAME'] = 'OUT'
    return gps

def get_haversine_distance(x1, y1, x2, y2):
    gps1 = (x1,y1)
    gps2 = (x2,y2)
    return (haversine(gps1, gps2)*1000)

if __name__ == "__main__":
    DepartureArrival()
