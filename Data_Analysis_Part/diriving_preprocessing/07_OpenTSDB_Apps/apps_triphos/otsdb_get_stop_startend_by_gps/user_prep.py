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
    ret = _getrest(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret
        
def _getrest(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}
    put_dps_dict1={}
    put_dps_dict2={}
    copied_ori_dict={}
    
    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname']=='GPS_LONG':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
            copied_ori_dict = copy.deepcopy(_dict)
        elif _dict['tags']['fieldname']=='GPS_LAT':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname']=='DRIVE_START':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname']=='DRIVE_END':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname']=='DRIVE_LENGTH_TOTAL':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        
    start_ts_list = list(total_dict['DRIVE_START'].keys())
    start_ts_list.sort()
    end_ts_list = list(total_dict['DRIVE_END'].keys())
    end_ts_list.sort()



    # 방문했다고 규정할 최소 시간 = 8분(600sec)
    T_min = 480
    # 데이터 결측(수집되지 않았다고 규정할 시간 = 1분(60sec)
    T_max = 60
    # 이동한것이 아니라고 분류할 최소 거리 = 15m
    D_max = 50

    LAT_times = set(total_dict['GPS_LAT'].keys())
    LONG_times = set(total_dict['GPS_LONG'].keys())
    timeStamp = list(LAT_times & LONG_times)
    timeStamp.sort()

    i=0
    startend_idx=0
    while startend_idx < len(end_ts_list) and i < len(timeStamp):
        start_ts = start_ts_list[startend_idx]
        end_ts = end_ts_list[startend_idx]
        while int(timeStamp[i]) < int(start_ts):
            i+=1
        
        while int(timeStamp[i]) < int(end_ts):
            j = i+1
            while j < len(timeStamp) and int(timeStamp[j]) <= int(end_ts):
                # get time diff j-1, j
                t = int(timeStamp[j])-int(timeStamp[j-1])
                if(t < 0):
                    t = -t
                # get gps distance j, i
                gps1 = (total_dict['GPS_LAT'][timeStamp[j]], total_dict['GPS_LONG'][timeStamp[j]])
                gps2 = (total_dict['GPS_LAT'][timeStamp[i]], total_dict['GPS_LONG'][timeStamp[i]])
                d = haversine(gps1,gps2)*1000 
                if t > T_max or d > D_max or int(timeStamp[j]) > int(end_ts):
                    # j-1, i
                    t = int(timeStamp[j-1])-int(timeStamp[i])
                    if(t < 0):
                        t = -t
                    if t > T_min:
                        # j-1, i
                        e_ts = timeStamp[j-1]
                        s_ts = timeStamp[i]
                        
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
                        if end_drive_length - start_drive_length <= 1 and start_drive_length != -1 and end_drive_length != -1:
                            # 데이터 손실에 의해 정차구간으로 지정되는 경우 배제
                            data_cnt=0
                            for stop_ts in range(int(s_ts), int(e_ts)+1):
                                try:
                                    total_dict['GPS_LAT'][str(stop_ts)]
                                    data_cnt+=1
                                except KeyError:
                                    continue
                            if data_cnt / float(int(e_ts)+1-int(s_ts)) * 100 > 90:
                                put_dps_dict1[s_ts] = 0
                                put_dps_dict2[e_ts] = 1
                    i = j
                    break
                j = j+1
            if j >= len(timeStamp) or timeStamp[j] > end_ts:
                break

        startend_idx+=1

    copied_ori_dict['dps'] = put_dps_dict1
    copied_ori_dict['tags']['fieldname'] = 'STOP_START'
    new_data_list.append(copied_ori_dict)

    copied_ori_dict = copy.deepcopy(copied_ori_dict)
    copied_ori_dict['dps'] = put_dps_dict2
    copied_ori_dict['tags']['fieldname'] = 'STOP_END'
    new_data_list.append(copied_ori_dict)
    
    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")
