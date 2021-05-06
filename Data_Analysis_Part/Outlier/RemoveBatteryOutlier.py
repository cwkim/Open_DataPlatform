# -*- coding:utf-8 -*-

import pandas as pd
import influxdb
from datetime import timedelta, datetime
import pymysql
import sys
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import joblib
from matplotlib.colors import LogNorm
from sklearn.mixture import GaussianMixture
import os
import secrets
sns.set()

def plot_centroids(centroids, weights=None, circle_color='w', cross_color='k'):
    if weights is not None:
        centroids = centroids[weights > weights.max() / 10]
    plt.scatter(centroids[:, 0], centroids[:, 1],
                marker='o', s=30, linewidths=8,
                color=circle_color, zorder=10, alpha=0.9)
    plt.scatter(centroids[:, 0], centroids[:, 1],
                marker='x', s=50, linewidths=50,
                color=cross_color, zorder=11, alpha=1)

def plot_gaussian_mixture(clusterer, X, resolution=1000, show_ylabels=True):
    mins = X.min(axis=0) - 0.1
    maxs = X.max(axis=0) + 0.1
    xx, yy = np.meshgrid(np.linspace(mins[0], maxs[0], resolution),
                         np.linspace(mins[1], maxs[1], resolution))
    Z = -clusterer.score_samples(np.c_[xx.ravel(), yy.ravel()])
    Z = Z.reshape(xx.shape)

    plt.contourf(xx, yy, Z,
                 norm=LogNorm(vmin=1.0, vmax=30.0),
                 levels=np.logspace(0, 2, 12))
    plt.contour(xx, yy, Z,
                norm=LogNorm(vmin=1.0, vmax=30.0),
                levels=np.logspace(0, 2, 12),
                linewidths=1, colors='k')

    Z = clusterer.predict(np.c_[xx.ravel(), yy.ravel()])
    Z = Z.reshape(xx.shape)
    plt.contour(xx, yy, Z,
                linewidths=2, colors='r', linestyles='dashed')

    plt.plot(X[:, 0], X[:, 1], 'k.', markersize=2)
    plot_centroids(clusterer.means_, clusterer.weights_)

    plt.xlabel("$x_1$", fontsize=14)
    if show_ylabels:
        plt.ylabel("$x_2$", fontsize=14, rotation=0)
    else:
        plt.tick_params(labelleft=False)

if __name__ == '__main__':

    #token = secrets.token_urlsafe(8)
    token = 'Project2'

    #1. 설정값
    #print('-----------------------------------------------------------------------------------------------------------')
    time_start = datetime.now()
    #print('Start /','Time:', time_start)

    #컨테이너 순서___받아와야함
    process_num =1
    date = (datetime.today() - timedelta(days=1)).strftime("^%Y-%m-%dT")
    #print('-----------------------------------------------------------------------------------------------------------')

    #2. 데이터 불러오기
    #print('\n데이터 불러오기')
    dataset = pd.read_csv('../Dataset/sample1.csv')

    #print('속성값:\n', dataset.columns)
    #print('\n헤드:\n', dataset.head())

    dataset_start = datetime.strptime(str(dataset['RECORD_TIME'][0])[0:19], '%Y-%m-%dT%H:%M:%S')
    dataset_end = datetime.strptime(str(dataset['RECORD_TIME'][len(dataset)-1])[0:19], '%Y-%m-%dT%H:%M:%S')

    dataset_dict = dataset.to_dict('records')
    date_list = []
    for i in range(len(dataset['RECORD_TIME'])):
        iter_date = datetime.strptime(str(dataset['RECORD_TIME'][i])[0:19], '%Y-%m-%dT%H:%M:%S')
        date_list.append(iter_date)
    dataset = dataset[['DRIVE_SPEED', 'PHONE_NUM', 'GPS_LAT', 'GPS_LONG', 'RPM', 'BATTERY_VOLTAGE', 'COOLANT_TEMPER', 'ENGINE_TORQUE']]
    dataset.index = date_list
    #print(dataset)

    #3. 증감량 데이터 생성
    #range가 필요함
    date = pd.date_range(dataset_start, dataset_end, freq="s")
    data = pd.DataFrame(index=date, columns=['DRIVE_SPEED', 'PHONE_NUM', 'GPS_LAT', 'GPS_LONG', 'RPM', 'BATTERY_VOLTAGE', 'COOLANT_TEMPER', 'ENGINE_TORQUE'])
    data.loc[dataset.index] = dataset

    AOC_VOLTAGE = data.BATTERY_VOLTAGE.sub(data.BATTERY_VOLTAGE.shift())
    #AOC_SPEED = data.DRIVE_SPEED.sub(data.DRIVE_SPEED.shift())
    AOC_VOLTAGE.name = 'AOC_VOLTAGE'
    #AOC_SPEED.name = 'AOC_SPEED'

    data = pd.concat([data, AOC_VOLTAGE], axis=1)
    data.dropna(axis=0)
    result_dataset = pd.concat([dataset, data])
    result_dataset = result_dataset.dropna(axis=0)

    #4. 가우시안 혼합 모델
    #Gaussian Mixture Model Load
    gmmodel = joblib.load("outlier_gm_battery.pkl")

    #Gaussian Mixture Model Outlier detection
    np_dataset = np.array(result_dataset[['BATTERY_VOLTAGE', 'AOC_VOLTAGE']])

    densities = gmmodel.score_samples(np_dataset)
    #print('densities:', densities)
    density_threshold = np.percentile(densities, 0.2)
    #print('density_threshold:', density_threshold)

    result_dataset['densities'] = densities
    #print('result_dataset:', result_dataset)
    result_dataset = result_dataset[result_dataset['densities'] > density_threshold]
    result_dataset = result_dataset.reset_index().rename(columns={"index": "RECORD_TIME"})
    result_dataset = result_dataset[['RECORD_TIME', 'DRIVE_SPEED', 'PHONE_NUM', 'GPS_LAT', 'GPS_LONG', 'RPM', 'BATTERY_VOLTAGE', 'COOLANT_TEMPER', 'ENGINE_TORQUE']]
    #print(result_dataset)

    #5. 그림을 통한 확인
    anomalies = np_dataset[densities < density_threshold]
    #print('anomalies:', anomalies)

    gm = GaussianMixture(n_components=1, n_init=10)
    gm.fit(np_dataset)

    plt.figure(figsize=(16, 10))

    plot_gaussian_mixture(gm, np_dataset)
    plt.scatter(anomalies[:, 0], anomalies[:, 1], color='r', marker='*')
    plt.title('BATTERY_VOLTAGE OUTLIER DETECTION (Gaussian Mixture)')
    plt.xlabel('BATTERY_VOLTAGE', fontsize=10)
    plt.ylabel('AOC_BATTERY_VOLTAGE', fontsize=10)

    #6. 결과 저장
    filename_str = "result_{}".format(process_num)
    result_dataset.to_csv('../{}/{}.csv'.format(token, filename_str))
    plt.savefig('../{}/{}.png'.format(token, filename_str))
    '''
    file = open('../{}/{}.txt'.format(token, filename_str), 'w')
    file.write('배터리 전압 이상치 탐지 결과 요약')
    file.write('\n데이터 수: '+ str(len(dataset['GPS_LAT'])))
    file.write('\n이상치 제거 후 데이터 수: ' + str(len(result_dataset['GPS_LAT'])))
    file.write('\n이상치 수: ' + str(len(anomalies)))
    file.write('\nGaussian Mixture Model\'s Percentile Threshold: 0.1')
    file.write('\nGaussian Mixture Model\'s Density Threshold: '+str(round(density_threshold,5)))
    file.write('\n이상치 제거 파일: {}.csv'.format(filename_str))
    file.write('\n이상치 제거 그림: {}.png'.format(filename_str))
    file.write('\nhttp://keti-ev.iptime.org:9001/{}/'.format(token))
    file.write('\n프로그램 시작 시간:' + str(time_start))
    file.write('\n프로그램 종료 시간:' + str(datetime.now()))
    '''

    print('배터리 전압 이상치 탐지 결과')
    print('\n데이터 수: ' + str(len(dataset['GPS_LAT'])))
    print('\n이상치 제거 후 데이터 수: ' + str(len(result_dataset['GPS_LAT'])))
    print('\n이상치 수: ' + str(len(anomalies)))
    print('\nGaussian Mixture Model\'s Percentile Threshold: 0.1')
    print('\nGaussian Mixture Model\'s Density Threshold: ' + str(round(density_threshold, 5)))
    print('\nhttp://keti-ev.iptime.org:9001/{}/{}.html'.format(token, filename_str))
    print('\n프로그램 시작 시간:' + str(time_start))
    print('\n프로그램 종료 시간:' + str(datetime.now()))

    html_text = '<!DOCTYPE html>'
    html_text += '\n<html>'
    html_text += '\n<head>'
    html_text += '\n    <title>일별 배터리전압 통계 추출</title>'
    html_text += '\n    <meta charset="utf-8">'
    html_text += '\n</head>'
    html_text += '\n'
    html_text += '\n<body>'
    html_text += '\n<h1> 일별 배터리전압 통계 추출 </h1>'
    html_text += '\n<p>'
    html_text += '\n데이터 수: '+ str(len(dataset['GPS_LAT'])) + '<br>'
    html_text += '\n이상치 제거 후 데이터 수: ' + str(len(result_dataset['GPS_LAT'])) + '<br>'
    html_text += '\n이상치 수: ' + str(len(anomalies)) + '<br>'
    html_text += '\nGaussian Mixture Model\'s Percentile Threshold: 0.1<br>'
    html_text += '\nGaussian Mixture Model\'s Density Threshold: ' + str(round(density_threshold, 5)) + '<br>'
    html_text += '\n<br>'

    html_text += '\n이상치 제거 파일: '
    html_text += '\n<A href = "{}.csv" target="blank"> result_1.csv</A>'.format(filename_str) + '<br>'
    html_text += '\n이상치 제거 그림: '
    html_text += '\n<A href = "{}.png" target="blank"> result_1.png</A>'.format(filename_str) + '<br>'

    html_text += '\n프로그램 시작 시간:' + str(time_start) + '<br>'
    html_text += '\n프로그램 종료 시간:' + str(datetime.now()) + '<br>'
    html_text += '\n<p/>'

    html_file = open('../{}/{}.html'.format(token, filename_str), 'w')
    html_file.write(html_text)
    html_file.close()