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
    ret = _fill_empty(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[11:13], _time[14:16], _time[17:])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))
    return epoch

def _fill_empty(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}
    put_dps_dict1 = {}
    put_dps_dict2 = {}
    
    for _dict in _dictbuf_list:
        if _dict['metric'] == 'Elex_driving_interval':
            if _dict['tags']['fieldname'] == 'DRIVE_LENGTH_TOTAL_1':
                copied_ori_dict1 = copy.deepcopy(_dict)
                total_dict[_dict['tags']['fieldname']] = _dict['dps']
            else:
                copied_ori_dict2 = copy.deepcopy(_dict)
                total_dict[_dict['tags']['fieldname']] = _dict['dps']
        else:
            total_dict[_dict['tags']['drive']] = _dict['dps']

    endkeys = total_dict['end'].keys()
    endkeys.sort()

    startkeys = total_dict['start'].keys()
    startkeys.sort()

    keylist = total_dict['DRIVE_LENGTH_TOTAL_1'].keys()
    for i in range(len(endkeys)):
        start = int(startkeys[i])
        end = int(endkeys[i])
        preidx=start
        while True:
            try:
                preval = total_dict['DRIVE_LENGTH_TOTAL_1'][str(preidx)]
                break
            except KeyError:
                preidx+=1
        for ts in range(start, end+1):
            try:
                put_dps_dict1[str(ts)] = total_dict['DRIVE_LENGTH_TOTAL_1'][str(ts)]
                preval = put_dps_dict1[str(ts)]
            except KeyError:
                put_dps_dict1[str(ts)] = preval
                preval = put_dps_dict1[str(ts)]

    keylist2 = total_dict['DRIVE_SPEED_1'].keys()
    for i in range(len(endkeys)):
        start = int(startkeys[i])
        end = int(endkeys[i])
        preidx=start
        while True:
            try:
                preval = total_dict['DRIVE_SPEED_1'][str(preidx)]
                break
            except KeyError:
                preidx+=1
        for ts in range(start, end+1):
            try:
                put_dps_dict2[str(ts)] = total_dict['DRIVE_SPEED_1'][str(ts)]
                preval = put_dps_dict2[str(ts)]
            except KeyError:
                put_dps_dict2[str(ts)] = preval
                preval = put_dps_dict2[str(ts)]

    copied_ori_dict1['dps'] = put_dps_dict1
    new_data_list.append(copied_ori_dict1)
    copied_ori_dict2['dps'] = put_dps_dict2
    new_data_list.append(copied_ori_dict2)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")
