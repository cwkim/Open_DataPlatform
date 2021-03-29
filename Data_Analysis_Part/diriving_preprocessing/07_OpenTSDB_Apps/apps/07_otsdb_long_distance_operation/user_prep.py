# -*- coding: utf-8 -*-
#!/usr/bin/env python2

from __future__ import print_function
import os
import sys
import time
import datetime
from collections import OrderedDict
import copy

# 호출되는 함수 
def prep(s_ut, e_ut, _dictbuf_list, _sec, meta):
    ret = long_distance(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[11:13], _time[14:16], _time[17:])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))
    return epoch

def long_distance(s_ut, e_ut, _dictbuf_list, _sec, meta):
    mean_length = 70.7
    mean_time = 27511
    mean_length_per_hour = 9.2

    new_data_list=[]
    total_dict={}
    put_dps_dict1 = {}
    put_dps_dict2 = {}
    
    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_LENGTH_TOTAL_1':
            copied_ori_dict1 = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = sorted(_dict['dps'].items())
        elif _dict['tags']['fieldname'] == 'DRIVE_SPEED_1':
            copied_ori_dict2 = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = sorted(_dict['dps'].items())


    for i in range(len(total_dict['DRIVE_LENGTH_TOTAL_1'])):
        if i == 0:
            start_time = total_dict['DRIVE_LENGTH_TOTAL_1'][i][0]
            start_length = total_dict['DRIVE_LENGTH_TOTAL_1'][i][1]
            start_num = i
            continue

        before_time = total_dict['DRIVE_LENGTH_TOTAL_1'][i-1][0]
        before_num = i-1
        current_time = total_dict['DRIVE_LENGTH_TOTAL_1'][i][0]
        
        if int(current_time) - int(before_time) > 1:
            diff_length = float(total_dict['DRIVE_LENGTH_TOTAL_1'][i-1][1]) - float(start_length)
            diff_time = int(total_dict['DRIVE_LENGTH_TOTAL_1'][i-1][0]) - int(start_time)
            length_per_hour = float(diff_length) / float(diff_time) * float(3600)
            if diff_length >= mean_length:
                if diff_time >= mean_time:
                    if length_per_hour >= mean_length_per_hour:
                        for _num in range(start_num, before_num+1):
                            put_dps_dict1[str(total_dict['DRIVE_LENGTH_TOTAL_1'][_num][0])] = total_dict['DRIVE_LENGTH_TOTAL_1'][_num][1]
                            put_dps_dict2[str(total_dict['DRIVE_SPEED_1'][_num][0])] = total_dict['DRIVE_SPEED_1'][_num][1]
            start_time = total_dict['DRIVE_LENGTH_TOTAL_1'][i][0]
            start_num = i
            start_length = total_dict['DRIVE_LENGTH_TOTAL_1'][i][1]
        
        if len(total_dict['DRIVE_LENGTH_TOTAL_1'])-1 == i:
            if diff_length >= mean_length:
                if diff_time >= mean_time:
                    if length_per_hour >= mean_length_per_hour:
                        for _num in range(start_num, before_num+1):
                            put_dps_dict1[str(total_dict['DRIVE_LENGTH_TOTAL_1'][_num][0])] = total_dict['DRIVE_LENGTH_TOTAL_1'][_num][1]
                            put_dps_dict2[str(total_dict['DRIVE_SPEED_1'][_num][0])] = total_dict['DRIVE_SPEED_1'][_num][1]

    tags_dict1 = { 'VEHICLE_NUM' : str(copied_ori_dict1['tags']['VEHICLE_NUM']), 'fieldname' : 'DRIVE_LENGTH_TOTAL_1', 'sort' : 'long_distance_operation' }
    tags_dict2 = { 'VEHICLE_NUM' : str(copied_ori_dict1['tags']['VEHICLE_NUM']), 'fieldname' : 'DRIVE_SPEED_1', 'sort' : 'long_distance_operation' }

    copied_ori_dict1['dps'] = put_dps_dict1
    copied_ori_dict1['tags'] = tags_dict1
    new_data_list.append(copied_ori_dict1)
    copied_ori_dict2['dps'] = put_dps_dict2
    copied_ori_dict2['tags'] = tags_dict2
    new_data_list.append(copied_ori_dict2)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")