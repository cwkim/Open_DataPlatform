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
    put_dps_dict3 = dict()
    new_data_list = []
    copied_ori_dict={}

    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_SPEED_1':
            copied_ori_dict = copy.deepcopy(_dict)
            total_dict['DRIVE_SPEED_1'] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'RPM_1':
            total_dict['RPM_1'] = _dict['dps']

    DSkeylist = total_dict['DRIVE_SPEED_1'].keys()
    DSkeylist.sort()
    RPMkeylist = total_dict['RPM_1'].keys()
    RPMkeylist.sort()

    start = min(int(DSkeylist[0]), int(RPMkeylist[0]))
    end = max(int(DSkeylist[-1]), int(RPMkeylist[-1]))

    for ts in range(start, end+1):
        try:
            if total_dict['DRIVE_SPEED_1'][str(ts)] > 0 and total_dict['RPM_1'][str(ts)] > 0:
                put_dps_dict1[str(ts)]=2
            elif total_dict['DRIVE_SPEED_1'][str(ts)] == 0 and total_dict['RPM_1'][str(ts)] > 0:
                put_dps_dict2[str(ts)]=1
            elif total_dict['DRIVE_SPEED_1'][str(ts)] == 0 and total_dict['RPM_1'][str(ts)] == 0:
                put_dps_dict3[str(ts)]=0
        except KeyError:
            continue
                
    
    copied_ori_dict['tags']['fieldname'] = 'CAR_STATUS'
    copied_ori_dict['tags']['work'] = 'driving'
    copied_ori_dict['dps']=put_dps_dict1
    new_data_list.append(copied_ori_dict)

    copied_ori_dict = copy.deepcopy(copied_ori_dict)
    copied_ori_dict['tags']['work'] = 'stop'
    copied_ori_dict['dps']=put_dps_dict2
    new_data_list.append(copied_ori_dict)

    copied_ori_dict = copy.deepcopy(copied_ori_dict)
    copied_ori_dict['tags']['work'] = 'parking'
    copied_ori_dict['dps']=put_dps_dict3
    new_data_list.append(copied_ori_dict)
    
    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list
        
if __name__ == "__main__" : 

    print ("working good")