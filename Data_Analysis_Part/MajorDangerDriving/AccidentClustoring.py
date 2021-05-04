# -*- coding:utf-8 -*-

import numpy as np
from haversine import haversine
from sklearn.cluster import MeanShift

def MajorCenters(_df, _bandwith):
    '''
    # input data
        _df : 위험운전 데이터
        _bandwith : Mean-Shift를 위한 bandwith 범위(resolution 고려해야함)
    # output data
        n_clusters_ : cluster 갯수
        center_value : cluster 결과의 중심 좌표

    '''
    strLat = list(_df["E_GPS_LAT"])
    strLong = list(_df["E_GPS_LONG"])

    gps_coordi = np.array([strLat[0], strLong[0]])

    for i in range(1, len(strLat)):
        _coordi = np.array([strLat[i], strLong[i]])
        gps_coordi = np.vstack((gps_coordi, _coordi))

    clustering = MeanShift(bandwidth=_bandwith).fit(gps_coordi)
    labels = clustering.labels_
    cluster_centers = clustering.cluster_centers_

    labels_unique = np.unique(labels)
    n_clusters_ = len(labels_unique)

    print("number of estimated clusters : %d" %n_clusters_)

    center_value = cluster_centers.tolist()

    return n_clusters_, center_value

def CenterMatching(df, _major_center, _id):

    share = len(df) // 4
    remainder = len(df) % 4

    if _id == '1':
        _start = 0
        _end = share
    elif _id == '2':
        _start = share
        _end = share * 2
    elif _id == '3':
        _start = share * 2
        _end = share * 3
    else:
        _start = share * 3
        _end = (share * 4) + remainder

    center_cla = []
    for i in range(_start, _end):
        print(_id,     i)
        serch_list = []
        for center_data in _major_center: 
            dist = haversine((df["E_GPS_LAT"][i], df["E_GPS_LONG"][i]), (center_data[0], center_data[1]))
            serch_list.append(dist)
        
        min_index = serch_list.index(min(serch_list))
        center_cla.append(min_index)

    return center_cla

