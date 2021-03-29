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
    ret = func(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

def func(s_ut, e_ut, _dictbuf_list, _sec, meta):
    total_dict = dict()
    put_dps_dict1 = dict()
    put_dps_dict2 = dict()
    new_data_list = []
    copied_ori_dict={}

    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_LENGTH_TOTAL':
            copied_ori_dict = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'CAR_STATUS':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']

    DPS_ts_list = list(total_dict['CAR_STATUS'].keys())
    DPS_ts_list.sort()
    
    i=0
    startts = -1
    endts = -1
    while i < len(total_dict['CAR_STATUS']):
        ts = DPS_ts_list[i]
        if startts == -1 and total_dict['CAR_STATUS'][ts] > 0:
            startts = ts
        elif startts != -1 and (total_dict['CAR_STATUS'][ts] == 0 or i == len(total_dict['CAR_STATUS'])-1):
            endts = ts
            # 10분 이상
            if int(endts) - int(startts) >= 600:
                start_drive_length=-1
                add=0
                while add < 600:
                    try:
                        start_drive_length = total_dict['DRIVE_LENGTH_TOTAL'][str(int(startts)+add)]
                        break
                    except KeyError:
                        add+=1

                end_drive_length=-1
                add=0
                while add < 600:
                    try:
                        end_drive_length = total_dict['DRIVE_LENGTH_TOTAL'][str(int(endts)-add)]
                        break
                    except KeyError:
                        add+=1
                # 2km 이상
                if end_drive_length != -1 and start_drive_length != -1 and end_drive_length - start_drive_length > 5:
                    put_dps_dict1[startts] = 0
                    put_dps_dict2[endts] = 1
            startts = -1
            endts = -1
        i+=1
                

    copied_ori_dict['dps']=put_dps_dict1
    copied_ori_dict['tags']['fieldname'] = 'DRIVE_START'
    new_data_list.append(copied_ori_dict)

    copied_ori_dict = copy.deepcopy(copied_ori_dict)
    copied_ori_dict['dps']=put_dps_dict2
    copied_ori_dict['tags']['fieldname'] = 'DRIVE_END'
    new_data_list.append(copied_ori_dict)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list
        
if __name__ == "__main__" : 

    print ("working good")