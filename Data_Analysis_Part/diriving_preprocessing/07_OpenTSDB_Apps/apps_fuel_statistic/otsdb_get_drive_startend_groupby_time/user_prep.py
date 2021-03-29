# -*- coding: utf-8 -*-
#!/usr/bin/env python2

from __future__ import print_function
import os
import sys
import time
import datetime
from collections import OrderedDict
import copy
#위도,경도 위치 계산 모듈 import
from haversine import haversine

# 호출되는 함수 
def prep(s_ut, e_ut, _dictbuf_list, _sec, meta):
    ret = get_time_interval(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

# convert Time to Epoch
def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[11:13], _time[14:16], _time[17:])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))
    return epoch

def convert_datetime(unixtime):
    """Convert unixtime to datetime"""
    import datetime
    date = datetime.datetime.fromtimestamp(unixtime).strftime('%Y-%m-%d %H:%M:%S')
    return date # format : str

def get_time_interval(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}
    i=0
    for _dict in _dictbuf_list:
        total_dict[_dict['tags']['fieldname']] = _dict['dps']
        if i==0:
            copied_ori_dict = copy.deepcopy(_dict)
        i+=1
    
    start_except_stop_keys = list(total_dict['DRIVE_START'].keys())
    start_except_stop_keys.sort()

    end_except_stop_keys = list(total_dict['DRIVE_END'].keys())
    end_except_stop_keys.sort()

    time_list=[] # 시간대가 저장될 리스트 
    hours = 24
    first = 0
    time_term = 4 # time_term은 24의 약수만 가능
    for i in range(hours/time_term+1):
        hour = first+i*time_term
        if hour < 10:
            time_list.append("0"+str(hour)+":00:00")
        else:
            time_list.append(str(hour)+":00:00")

    # time_term = 4 일때 time_list 결과 => ["00:00:00", "04:00:00", "08:00:00", "12:00:00", "16:00:00", "20:00:00", "24:00:00"]
    #print(time_list)
    
    put_dps_dict1=[]
    put_dps_dict2=[]
    for i in range(hours/time_term):
        put_dps_dict1.append({})
        put_dps_dict2.append({})

    i=0
    while i < len(end_except_stop_keys):
        if int(start_except_stop_keys[i]) > int(end_except_stop_keys[i]):
            i+=1
            continue
        str_start = convert_datetime(int(start_except_stop_keys[i]))
        str_end = convert_datetime(int(end_except_stop_keys[i]))
        print(str_start, str_end, start_except_stop_keys[i], end_except_stop_keys[i])
        str_start_time = str_start[11:]
        str_end_time = str_end[11:]
        j=0
        while j < len(time_list)-1:
            if str_start_time >= time_list[j] and str_start_time < time_list[j+1]:
                if str_end_time < time_list[j+1] and str_start_time < str_end_time :
                    put_dps_dict1[j][start_except_stop_keys[i]]=2*j
                    put_dps_dict2[j][end_except_stop_keys[i]]=2*j+1
                    print(start_except_stop_keys[i], end_except_stop_keys[i])
                else :
                    put_dps_dict1[j][start_except_stop_keys[i]]=2*j
                    if j+1 < len(time_list)-1:
                        end_ts = convertTimeToEpoch(str_start[0:11] + time_list[j+1])
                    else:
                        end_ts = convertTimeToEpoch(str_end[0:11] + time_list[0])
                    put_dps_dict2[j][str(end_ts-1)]=2*j+1
                    print(start_except_stop_keys[i], str(end_ts-1))
                    if j+1 >= len(put_dps_dict1):
                        put_dps_dict1[0][str(end_ts)]=2*0
                        put_dps_dict2[0][end_except_stop_keys[i]]=2*0+1
                    else :
                        put_dps_dict1[j+1][str(end_ts)]=2*(j+1)
                        put_dps_dict2[j+1][end_except_stop_keys[i]]=2*(j+1)+1
                    print(str(end_ts), end_except_stop_keys[i])
                break
            j+=1
        i+=1

    for i in range(len(put_dps_dict1)):
        copied_ori_dict1 = copy.deepcopy(copied_ori_dict)
        copied_ori_dict1['dps'] = put_dps_dict1[i]
        copied_ori_dict1['tags']['fieldname'] = 'DRIVE_START'
        copied_ori_dict1['tags']['SORT'] = time_list[i][0:2] + '_' + time_list[i+1][0:2]
        new_data_list.append(copied_ori_dict1)

        copied_ori_dict2 = copy.deepcopy(copied_ori_dict)
        copied_ori_dict2['dps'] = put_dps_dict2[i]
        copied_ori_dict2['tags']['fieldname'] = 'DRIVE_END'
        copied_ori_dict2['tags']['SORT'] = time_list[i][0:2] + '_' + time_list[i+1][0:2]
        new_data_list.append(copied_ori_dict2)
    
    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")
