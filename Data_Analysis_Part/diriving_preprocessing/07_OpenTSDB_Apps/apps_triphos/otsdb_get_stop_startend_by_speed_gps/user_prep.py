# -*- coding: utf-8 -*-
#!/usr/bin/env python2

from __future__ import print_function
import os
import sys
import time
import datetime
from collections import OrderedDict
import copy
from haversine import haversine

# 호출되는 함수 
def prep(s_ut, e_ut, _dictbuf_list, _sec, meta):
    ret = _getstop(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret
        
def _getstop(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}
    put_dps_dict1={}
    put_dps_dict2={}
    
    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_START':
            copied_ori_dict1 = copy.deepcopy(_dict)
            copied_ori_dict2 = copy.deepcopy(_dict)
            copied_ori_dict3 = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'DRIVE_END':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'DRIVE_SPEED':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'DRIVE_LENGTH_TOTAL':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'GPS_LAT':
            total_dict[_dict['tags']['fieldname']]= _dict['dps']
        elif _dict['tags']['fieldname'] == 'GPS_LONG':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']

    #각 주행 구간의 시작 지점, 종료 지점 list로 저장
    endkeys = total_dict['DRIVE_END'].keys()
    endkeys.sort()
    startkeys = total_dict['DRIVE_START'].keys()
    startkeys.sort()
    DS_key_list = total_dict['DRIVE_SPEED'].keys()
    DS_key_list.sort()

    i = 0 # gps idx
    ii = 0 # speed idx
    #GPS_LAT, GPS_LONG 필드의 timestamp list 로 저장(KeyError방지를 위해)
    gps_lat_ts=set(total_dict['GPS_LAT'].keys())
    gps_long_ts = set(total_dict['GPS_LONG'].keys())
    total_gps_ts=list(gps_lat_ts&gps_long_ts)
    total_gps_ts.sort()

    T_min,T_max,D_max = 480,60,50

    #주행 구간 탐색
    for k in range(len(endkeys)):
        rest_ts_dic = {}

        #rest_ts_dic 0으로 초기화
        for ts in range(int(startkeys[k]),int(endkeys[k])+1):
            rest_ts_dic[str(ts)]=0

        rest_ts=rest_ts_dic.keys()
        gps_ts=list(set(total_gps_ts)&set(rest_ts))
        gps_ts.sort()
        
        #정차 구간 gps 알고리즘을 통해 추출
        i=0
        while i < len(gps_ts):
            j=i+1
            while j < len(gps_ts):
                # get time diff j-1, j
                t = int(gps_ts[j - 1]) - int(gps_ts[j])
                if (t < 0):
                    t = -t
                # get gps distance j, i
                gps1 = (total_dict['GPS_LAT'][gps_ts[j]], total_dict['GPS_LONG'][gps_ts[j]])
                gps2 = (total_dict['GPS_LAT'][gps_ts[i]], total_dict['GPS_LONG'][gps_ts[i]])

                d = haversine(gps1, gps2) * 1000

                if t > T_max or d > D_max:
                    # j-1, i
                    t = int(gps_ts[j - 1]) - int(gps_ts[i])
                    if (t < 0):
                        t = -t
                    if t >= T_min:
                        s_ts = gps_ts[i]
                        e_ts = gps_ts[j - 1]
                        #누적주행거리 차가 1보다 작은 경우 추가
                        end_drive_length = -1
                        start_drive_length = -1
                        add = 0
                        while add < (int(e_ts)-int(s_ts))/2:
                            try:
                                end_drive_length = total_dict['DRIVE_LENGTH_TOTAL'][str(int(e_ts)-add)]
                                break
                            except KeyError:
                                add+=1
                        add = 0
                        while add < (int(e_ts)-int(s_ts))/2:
                            try:
                                start_drive_length = total_dict['DRIVE_LENGTH_TOTAL'][str(int(s_ts)+add)]
                                break
                            except KeyError:
                                add+=1
                        if end_drive_length - start_drive_length < 1 and start_drive_length != -1 and end_drive_length != -1:
                            # 데이터 손실에 의해 정차구간으로 지정되는 경우 배제
                            data_cnt=0
                            for stop_ts in range(int(s_ts), int(e_ts)+1):
                                try:
                                    total_dict['GPS_LAT'][str(stop_ts)]
                                    data_cnt+=1
                                except KeyError:
                                    continue
                            if data_cnt / float(int(e_ts)+1-int(s_ts)) * 100 > 90:
                                #gps로 구한 정차 구간의 값을 1로 저장
                                for ts in range(int(s_ts), int(e_ts)+1):
                                    rest_ts_dic[str(ts)]=1
                                #print("add gps range %s ~ %s" % (e_ts,s_ts))
                    i = j
                    break
                j = j + 1

            if j == len(gps_ts):
                break

        start = startkeys[k]
        end = endkeys[k]

        while ii < len(DS_key_list) and DS_key_list[ii] < start:
            ii+=1

        zero_start = -1
        zero_end = -1
        while ii < len(DS_key_list) and DS_key_list[ii] < end:
            if total_dict['DRIVE_SPEED'][DS_key_list[ii]] == 0 and zero_start == -1:
                zero_start = DS_key_list[ii]
            elif (zero_start != -1 and total_dict['DRIVE_SPEED'][DS_key_list[ii]] != 0) or (zero_start != -1 and (DS_key_list[ii+1] >= end or ii+1 == len(DS_key_list))):
                if total_dict['DRIVE_SPEED'][DS_key_list[ii]] != 0:
                    zero_end = DS_key_list[ii-1]
                else:
                    zero_end = DS_key_list[ii]
                zero_start = int(zero_start)
                zero_end = int(zero_end)
                if zero_end - zero_start >= 480:
                    #누적주행거리 차가 1보다 작은 경우 추가
                    
                    end_drive_length = -1
                    start_drive_length = -1
                    add = 0
                    while add < (zero_end-zero_start)/2:
                        try:
                            end_drive_length = total_dict['DRIVE_LENGTH_TOTAL'][str(zero_end-add)]
                            break
                        except KeyError:
                            add+=1
                    add = 0
                    while add < (zero_end-zero_start)/2:
                        try:
                            start_drive_length = total_dict['DRIVE_LENGTH_TOTAL'][str(zero_start+add)]
                            break
                        except KeyError:
                            add+=1
                    # speed로 구한 정차 구간의 값을 1로 변경
                    if end_drive_length - start_drive_length <= 1 and start_drive_length != -1 and end_drive_length != -1:
                        #print(zero_start, zero_end)
                        data_cnt=0
                        # 데이터 손실에 의해 정차구간으로 지정되는 경우 배제
                        for stop_ts in range(zero_start, zero_end+1):
                            try:
                                total_dict['DRIVE_SPEED'][str(stop_ts)]
                                data_cnt+=1
                            except KeyError:
                                continue
                        if data_cnt / float(zero_end+1-zero_start) * 100 > 90:
                            for ts in range(zero_start, zero_end+1):
                                rest_ts_dic[str(ts)]=1

                zero_start = -1
                zero_end = -1
            ii+=1

        rest_s=-1
        rest_e=-1
        #gps, speed 를 통해 구한 정차 구간 union, put dps
        for ts in range(int(startkeys[k]),int(endkeys[k])+1):
            if rest_ts_dic[str(ts)] == 1 and rest_s == -1:
                rest_s=ts
            elif ts == endkeys[k] and rest_ts_dic[str(ts)] == 1 and rest_s != -1:
                rest_e = ts
                put_dps_dict1[str(rest_s)] = 0
                put_dps_dict2[str(rest_e)] = 1
            elif rest_ts_dic[str(ts)] != 1 and rest_s != -1:
                rest_e = ts-1
                put_dps_dict1[str(rest_s)] = 0
                put_dps_dict2[str(rest_e)] = 1
                rest_s = -1
                rest_e = -1

    copied_ori_dict1['dps'] = put_dps_dict1
    copied_ori_dict1['tags']['fieldname'] = 'STOP_START'
    new_data_list.append(copied_ori_dict1)

    copied_ori_dict2['dps'] = put_dps_dict2
    copied_ori_dict2['tags']['fieldname'] = 'STOP_END'
    new_data_list.append(copied_ori_dict2)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")
