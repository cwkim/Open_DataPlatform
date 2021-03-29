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
    ret = _getrest(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret
        
def _getrest(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}
    put_dps_dict={}

    for _dict in _dictbuf_list:
        if _dict['metric'] == 'Elex_driving_startend_v2':
            total_dict[_dict['tags']['drive']] = _dict['dps']
        elif _dict['metric'] == 'Elex_data_full_v3':
            if _dict['tags']['fieldname'] == 'DRIVE_SPEED_1':
                copied_ori_dict = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = _dict['dps']

    endkeys = list(total_dict['end'].keys())
    endkeys.sort()
    startkeys = list(total_dict['start'].keys())
    startkeys.sort()
    
    
    for i in range(len(endkeys)):
        zero_start=-1
        zero_end=-1
        for ts in range(int(startkeys[i]), int(endkeys[i])+1):
            if total_dict['DRIVE_SPEED_1'][str(ts)] == 0 and zero_start == -1:
                 zero_start = ts
            elif total_dict['DRIVE_SPEED_1'][str(ts)] != 0 and zero_start != -1:
                if ts == int(endkeys[i]):
                    zero_end = ts
                else:
                    zero_end = ts-1
                if zero_end - zero_start >= 600 and total_dict['DRIVE_LENGTH_TOTAL_1'][str(zero_end)] - total_dict['DRIVE_LENGTH_TOTAL_1'][str(zero_start)] == 0:
                    for timestamp in range(zero_start, zero_end+1):
                        put_dps_dict[str(timestamp)] = 0
                zero_start=-1
                zero_end=-1
                

    copied_ori_dict['dps'] = put_dps_dict
    copied_ori_dict['tags']['work'] = 'rest'
    new_data_list.append(copied_ori_dict)
    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")
