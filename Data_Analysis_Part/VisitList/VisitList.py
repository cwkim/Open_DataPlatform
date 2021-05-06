# -*- coding:utf-8 -*-

"""
    Title : VisitList_Rulebase_Geofencing.py
    Comment : 차량의 방문을 추출한 후 Geofencing을 통해 공장 방문여부를 판단함_Realtime
    Author : 최우정 / github : https://github.com/woojungchoi
"""

import pandas as pd
from datetime import timedelta, datetime
from haversine import haversine
import folium
import time

def get_time_difference(p1, p2):
    #time difference to total seconds
    timediff = (p1 - p2).total_seconds()
    if (timediff<0): timediff = -timediff

    return timediff

def get_haversine_distance(x1, y1, x2, y2):
    gps1 = (x1, y1)
    gps2 = (x2, y2)

    return (haversine(gps1, gps2)*1000)

def get_visitlist_rulebase(data, Tmin, Tmax, Dmax):
    i = 1
    v_list = []
    while i < len(data['RECORD_TIME']):
        j = i + 1
        while j < len(data['RECORD_TIME']):
            t = get_time_difference(data['RECORD_TIME'][j - 1], data['RECORD_TIME'][j])
            d = get_haversine_distance(data['GPS_LAT'][j], data['GPS_LONG'][j], data['GPS_LAT'][i],
                                            data['GPS_LONG'][i])
            if (t > Tmax) or (d > Dmax):
                t = get_time_difference(data['RECORD_TIME'][j - 1], data['RECORD_TIME'][i])
                if (t > Tmin):
                    # print('visit_list: ', i, j - 1)
                    p1, p2 = j - 1, i

                    lat, lng = 0, 0
                    for k in range(p2, p1 + 1):
                        lat += data['GPS_LAT'][k]
                        lng += data['GPS_LONG'][k]
                    lat /= (p1 - p2 + 1)
                    lng /= (p1 - p2 + 1)

                    if lat > 300:
                        lat /= 10
                        lng /= 10

                    temp_visit = [round(lat, 6), round(lng, 6)]

                    # for influxdb datetime index
                    temp_visit.append(data['RECORD_TIME'][i])
                    # start time
                    time_start = data['RECORD_TIME'][i].strftime('%Y-%m-%dT%H:%M:%SZ')
                    temp_visit.append(time_start)
                    # end time
                    time_end = data['RECORD_TIME'][j - 1].strftime('%Y-%m-%dT%H:%M:%SZ')
                    temp_visit.append(time_end)

                    v_list.append(temp_visit)
                i = j
                break
            j = j + 1
        if (j == len(data['RECORD_TIME'])): break
    # 방문 결과가 없을 경우
    if not v_list: return list()
    v_df = pd.DataFrame(v_list)
    v_df = v_df.set_index(2)
    col_rename = {0: 'GPS_LAT', 1: 'GPS_LONG', 3: 'VISIT_START', 4: 'VISIT_END', 5: 'FACTORY_NUM', 6: 'FACTORY_NAME'}
    v_df = v_df.rename(columns=col_rename)

    return v_df

if __name__ == '__main__':

    # token = secrets.token_urlsafe(8)
    token = 'Project1'

    #1. 설정값
    #print('-----------------------------------------------------------------------------------------------------------')
    time_start = datetime.now()
    #print('Start /','Time:', time_start)

    #컨테이너 순서
    process_num =2

    #방문했다고 규정할 최소 시간 = 10분(600sec)
    Tmin = 600
    #데이터 결측(수집되지 않았다고 규정할 시간) = 10분(600sec)
    Tmax = 600
    #이동한것이 아니라고 분류할 최소 거리 = 250 단위:m
    Dmax = 250

    date = (datetime.today() - timedelta(days=1)).strftime("^%Y-%m-%dT")
    #print('-----------------------------------------------------------------------------------------------------------')


    #2. 데이터 불러오기
    #print('\n데이터 불러오기')
    while True:
        try:
            dataset = pd.read_csv('../{}/result_1.csv'.format(token))
            break
        except:
            time.sleep(3)

    #print('속성값:\n', dataset.columns)
    ##print('\n헤드:\n', dataset.head())
    print('\n통계값:\n', dataset.describe())

    dataset = dataset.to_dict()
    for i in range(len(dataset['RECORD_TIME'])):
        dataset['RECORD_TIME'][i] = datetime.strptime(str(dataset['RECORD_TIME'][i][0:19]), '%Y-%m-%d %H:%M:%S')
    #print('-----------------------------------------------------------------------------------------------------------')


    #3. 방문리스트 도출
    #print('\n방문리스트 도출')
    visit_list = get_visitlist_rulebase(dataset, Tmin, Tmax, Dmax)
    visit_list = visit_list.reset_index(drop=True)
    #print('도출 방문지 수:', len(visit_list['GPS_LAT']))


    #4. 방문리스트 지도 생성
    #print('\n방문리스트 지도 생성')
    map = folium.Map(location = (visit_list['GPS_LAT'].mean(),visit_list['GPS_LONG'].mean()), zoom_start=13)
    visit_dict = visit_list.to_dict(orient='index')

    for i in range(len(visit_list)):
        #지도 중심 설정
        visit_tuple = [visit_dict[i]['GPS_LAT'],visit_dict[i]['GPS_LONG']]

        #지도 popup창 설정
        visit_start = datetime.strptime(str(visit_dict[i]['VISIT_START']), '%Y-%m-%dT%H:%M:%SZ')
        visit_start = visit_start.strftime('%Y년%m월%d일 %H:%M:%S')
        visit_end = datetime.strptime(str(visit_dict[i]['VISIT_END']), '%Y-%m-%dT%H:%M:%SZ')
        visit_end = visit_end.strftime('%Y년%m월%d일 %H:%M:%S')
        popupstr = '방문번호: ' + str(i) + '<br>방문시작:\n' + visit_start + '<br>방문종료:\n' + visit_end

        #popup창 크기 설정
        folium.Marker(visit_tuple, popup = folium.Popup(popupstr, max_width=300)).add_to(map)


    # 5. 결과 저장

    filename_str = "result_{}".format(process_num)
    visit_list.to_csv('../{}/{}.csv'.format(token,filename_str))
    map.save('../{}/{}_img.html'.format(token,filename_str))
    '''
    file = open('../{}/{}.txt'.format(token,filename_str), 'w')
    file.write('방문지 도출 결과')
    file.write('\n데이터 수: '+ str(len(dataset['GPS_LAT'])))
    file.write('\n방문 수: '+ str(len(visit_list['GPS_LAT'])))
    file.write('\n방문목록 파일: {}.csv'.format(filename_str))
    file.write('\n방문목록 지도: {}.html'.format(filename_str))
    file.write('\nhttp://keti-ev.iptime.org:9001/{}/'.format(token))
    file.write('\n프로그램 시작 시간:'+ str(time_start))
    file.write('\n프로그램 종료 시간:'+ str(datetime.now()))
    '''

    print('방문지 도출 결과 요약')
    print('\n데이터 수: ' + str(len(dataset['GPS_LAT'])))
    print('\n방문 수: ' + str(len(visit_list['GPS_LAT'])))
    print('\nhttp://keti-ev.iptime.org:9001/{}/{}.html'.format(token, filename_str))
    print('\n프로그램 시작 시간:' + str(time_start))
    print('\n프로그램 종료 시간:' + str(datetime.now()))

    html_text = '<!DOCTYPE html>'
    html_text += '\n<html>'
    html_text += '\n<head>'
    html_text += '\n    <title>방문지 도출 결과</title>'
    html_text += '\n    <meta charset="utf-8">'
    html_text += '\n</head>'
    html_text += '\n'
    html_text += '\n<body>'
    html_text += '\n<h1> 방문지 도출 결과 </h1>'
    html_text += '\n<p>'
    html_text += '\n데이터 수: ' + str(len(dataset['GPS_LAT'])) + '<br>'
    html_text += '\n방문 수: ' + str(len(visit_list['GPS_LAT'])) +'<br>'
    html_text += '\n<br>'

    html_text += '\n방문목록 파일: '
    html_text += '\n<A href = "{}.csv" target="blank"> result_2.csv</A>'.format(filename_str) + '<br>'
    html_text += '\n방문목록 지도: '
    html_text += '\n<A href = "{}_img.html" target="blank"> result_2_img.html</A>'.format(filename_str) + '<br>'

    html_text += '\n프로그램 시작 시간:' + str(time_start) + '<br>'
    html_text += '\n프로그램 종료 시간:' + str(datetime.now()) + '<br>'
    html_text += '\n<p/>'

    html_file = open('../{}/{}.html'.format(token, filename_str), 'w')
    html_file.write(html_text)
    html_file.close()