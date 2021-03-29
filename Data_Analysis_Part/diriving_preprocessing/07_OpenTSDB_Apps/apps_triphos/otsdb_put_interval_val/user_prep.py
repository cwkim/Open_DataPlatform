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
    new_data_list = []
    put_dps_dict={}
    copied_ori_dict={}

    start_dict={}
    end_dict={}
    output_fieldname = 'STOP_BY_DPS'

    n=0
    for _dict in _dictbuf_list:
        if n == 0:
            copied_ori_dict = copy.deepcopy(_dict)
            n+=1
        if _dict['tags']['fieldname'] == 'DRIVE_START':
            start_dict = _dict['dps']
        elif _dict['tags']['fieldname'] == 'DRIVE_END':
            end_dict = _dict['dps']
        elif _dict['tags']['fieldname'] == 'STOP_START':
            start_dict = _dict['dps']
        elif _dict['tags']['fieldname'] == 'STOP_END':
            end_dict = _dict['dps']
        elif _dict['tags']['fieldname'] == 'PARKING_START':
            start_dict = _dict['dps']
        elif _dict['tags']['fieldname'] == 'PARKING_END':
            end_dict = _dict['dps']

    start_ts_list = list(start_dict.keys())
    start_ts_list.sort()
    end_ts_list = list(end_dict.keys())
    end_ts_list.sort()

    i=0
    while i < len(end_ts_list):
        start_ts = start_ts_list[i]
        end_ts = end_ts_list[i]
        for ts in range(int(start_ts), int(end_ts)+1):
            put_dps_dict[str(ts)]=2
        i+=1

    copied_ori_dict['tags']['fieldname'] = output_fieldname
    copied_ori_dict['dps'] = put_dps_dict
    new_data_list.append(copied_ori_dict)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list
        
if __name__ == "__main__" : 

    print ("working good")