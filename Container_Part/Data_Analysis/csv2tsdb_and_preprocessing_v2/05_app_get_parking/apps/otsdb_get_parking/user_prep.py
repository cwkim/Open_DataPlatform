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
    ret = _parking(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

        
def _parking(s_ut, e_ut, _dictbuf_list, _sec, meta):
    length_diff = 1

    total_dict = dict()
    put_dps_dict1 = dict()
    put_dps_dict2 = dict()

    new_data_list = []

    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_SPEED':
            copied_ori_dict = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
  
    sorted_spd = list(sorted(total_dict['DRIVE_SPEED'].items()))
    
    for i in range(len(total_dict['DRIVE_SPEED'])):
        if i == 0: continue

        before_time = sorted_spd[i-1][0]
        current_time = sorted_spd[i][0]
        if int(current_time) - int(before_time) > 7200:
            x=y=0
            while True:
                try:
                    pre_ts=str(int(before_time) + x)
                    before_spd=int(total_dict['DRIVE_SPEED'][pre_ts])
                except KeyError:
                    x+=1
                    continue
                try:
                    cur_ts=str(int(current_time) - y)
                    current_spd = int(total_dict['DRIVE_SPEED'][cur_ts])
                    break
                except KeyError:
                    y+=1
                    continue

            if before_spd <= 5 and current_spd <= 5:
                put_dps_dict1[before_time] = 0
                put_dps_dict2[current_time] =1

    copied_ori_dict['dps'] = put_dps_dict1
    copied_ori_dict['tags']['fieldname'] = 'PARK_START'
    new_data_list.append(copied_ori_dict)

    copied_ori_dict = copy.deepcopy(copied_ori_dict)
    copied_ori_dict['dps'] = put_dps_dict2
    copied_ori_dict['tags']['fieldname'] = 'PARK_END'
    new_data_list.append(copied_ori_dict)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list


if __name__ == "__main__" : 

    print ("working good")