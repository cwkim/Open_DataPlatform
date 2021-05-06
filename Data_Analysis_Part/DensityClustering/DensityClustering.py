# -*- coding:utf-8 -*-

import pandas as pd
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt
import time
from datetime import timedelta, datetime
import numpy as np
from datetime import timedelta, datetime
import math
import numbers
import folium
import json
from pymongo import MongoClient
import seaborn as sns

def get_centroid(dict_data):
    lat, lng = 0, 0
    for k in range(0, len(dict_data['GPS_LAT'])):
        lat += dict_data['GPS_LAT'][k]
        lng += dict_data['GPS_LONG'][k]
    lat /= len(dict_data['GPS_LAT'])
    lng /= len(dict_data['GPS_LAT'])

    return [round(lat, 6), round(lng, 6)]

if __name__ == '__main__':

    # token = secrets.token_urlsafe(8)
    token = 'Project1'

    #1. 설정값
    #print('-----------------------------------------------------------------------------------------------------------')
    time_start = datetime.now()
    #print('Start /','Time:', time_start)

    #__컨테이너 순서
    process_num =3
    #print('-----------------------------------------------------------------------------------------------------------')


    #2. 데이터 불러오기
    #print('\n데이터 불러오기')
    while True:
        try:
            dataset = pd.read_csv('../{}/result_2.csv'.format(token))
            break
        except:
            time.sleep(3)
    #print('속성값:\n', dataset.columns)
    #print('\n헤드:\n', dataset.head())
    #print('\n통계값:\n', dataset.describe())

    dataset = dataset[['GPS_LAT', 'GPS_LONG']]
    dataset_dict = dataset.to_dict()
    #print('-----------------------------------------------------------------------------------------------------------')

    #3. DBSCAN
    #___DBSCAN 조건 설정
    #print('Density_Based_Clustering_Algorithm')
    eps = 0.0005
    min_samples = int(len(dataset['GPS_LAT']) / 4)
    #print('eps:', eps)
    #print('min_samples', min_samples)

    #__DBSCAN 모델 생성
    model = DBSCAN(eps=eps, min_samples=min_samples)
    model.fit(dataset)
    y_predict = model.fit_predict(dataset)
    dataset['cluster']=y_predict
    dataset['cluster'] = dataset['cluster'] + 1
    num_cluster = max(dataset['cluster'].unique())

    #__클러스터 색상
    colors = sns.color_palette("colorblind").as_hex()
    colors[0] = 'black'

    #__클러스터 결과 확인
    #print('\n군집 수:', num_cluster)
    for i in range(num_cluster):
        #print('\t군집_{}'.format(i+1))
        center = (round(dataset[dataset['cluster']==0]['GPS_LAT'].mean(),6),round(dataset[dataset['cluster']==0]['GPS_LONG'].mean(),6))
        #print('\tCenter:', center)


    #4. 군집결과 지도 생성
    #print('\n군집화 지도 생성')
    map = folium.Map(location=(dataset['GPS_LAT'].mean(), dataset['GPS_LONG'].mean()), tiles='Stamen Terrain', zoom_start=13)
    dataset_dict = dataset.to_dict(orient='index')

    for i in range(len(dataset['GPS_LAT'])):
        gps_tuple = [round(dataset_dict[i]['GPS_LAT'],6), round(dataset_dict[i]['GPS_LONG'],6)]
        cluster_num = dataset_dict[i]['cluster']
        popupstr = '번호:{}<br>군집:{}<br>위치:{}'.format(i, cluster_num, str(gps_tuple))

        folium.CircleMarker(gps_tuple, color=colors[cluster_num], fill_color=colors[cluster_num], radius=2.5,
                            popup = folium.Popup(popupstr, max_width=300)).add_to(map)


    #5. 결과 저장
    filename_str = "result_{}".format(process_num)
    dataset.to_csv('../{}/{}.csv'.format(token, filename_str))
    map.save('../{}/{}_img.html'.format(token, filename_str))
    #file = open('../{}/{}.txt'.format(token, filename_str), 'w')
    #file.write('밀도기반 클러스터링 방법을 이용한 거점 도출결과 요약')
    #file.write('\n데이터 수: '+ str(len(dataset['GPS_LAT'])))
    #file.write('\n군집 수: '+ str(num_cluster))
    print('밀도기반 클러스터링 방법을 이용한 거점 도출결과')
    print('\n데이터 수: ' + str(len(dataset['GPS_LAT'])))
    print('\n군집 수: ' + str(num_cluster))

    for i in range(num_cluster):
        #file.write('\n\t군집_{}'.format(i+1))
        print('\n\t군집_{}'.format(i+1))
        center = (round(dataset[dataset['cluster']==0]['GPS_LAT'].mean(),6),round(dataset[dataset['cluster']==0]['GPS_LONG'].mean(),6))
        #file.write('\tCenter:'+ str(center))
        print('\tCenter:'+ str(center))
    #file.write('\n군집결과 파일: {}.csv'.format(filename_str))
    #file.write('\n군집결과 지도: {}_img.html'.format(filename_str))
    #file.write('\nhttp://keti-ev.iptime.org:9001/{}/'.format(token))
    #file.write('\n프로그램 시작 시간:' + str(time_start))
    #file.write('\n프로그램 종료 시간:' + str(datetime.now()))

    print('\nhttp://keti-ev.iptime.org:9001/{}/{}.html'.format(token, filename_str))
    print('\n프로그램 시작 시간:' + str(time_start))
    print('\n프로그램 종료 시간:' + str(datetime.now()))

    html_text = '<!DOCTYPE html>'
    html_text += '\n<html>'
    html_text += '\n<head>'
    html_text += '\n    <title>밀도기반 클러스터링 방법을 이용한 거점 도출결과</title>'
    html_text += '\n    <meta charset="utf-8">'
    html_text += '\n</head>'
    html_text += '\n'
    html_text += '\n<body>'
    html_text += '\n<h1> 밀도기반 클러스터링 방법을 이용한 거점 도출결과 </h1>'
    html_text += '\n<p>'
    html_text += '\n데이터 수: '+ str(len(dataset['GPS_LAT'])) + '<br>'
    html_text += '\n군집 수: '+ str(num_cluster) +'<br>'
    html_text += '\n<br>'

    for i in range(num_cluster):
        html_text += '\n\t군집_{}'.format(i+1) + '<br>'
        center = (round(dataset[dataset['cluster'] == 0]['GPS_LAT'].mean(), 6),
                  round(dataset[dataset['cluster'] == 0]['GPS_LONG'].mean(), 6))
        html_text += '\tCenter:'+ str(center) + '<br>'

    html_text += '\n군집결과 파일: '
    html_text += '\n<A href = "{}.csv" target="blank"> result_3.csv</A>'.format(filename_str) + '<br>'
    html_text += '\n군집결과 지도: '
    html_text += '\n<A href = "{}_img.html" target="blank"> result_3_img.html</A>'.format(filename_str) + '<br>'

    html_text += '\n프로그램 시작 시간:' + str(time_start) + '<br>'
    html_text += '\n프로그램 종료 시간:' + str(datetime.now()) + '<br>'
    html_text += '\n<p/>'

    html_file = open('../{}/{}.html'.format(token, filename_str), 'w')
    html_file.write(html_text)
    html_file.close()