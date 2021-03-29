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
    ret = _parking_v2(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

def _parking(s_ut, e_ut, _dictbuf_list, _sec, meta):
    total_dict = dict()
    put_dps_dict = dict()

    new_data_list = []

    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_SPEED_1':
            copied_ori_dict = copy.deepcopy(_dict)
            sorted_spd = sorted(_dict['dps'].items())
            total_dict[_dict['tags']['fieldname']] = sorted_spd
        else:
            total_dict[_dict['tags']['fieldname']] = _dict['dps']

    for i in range(len(total_dict['DRIVE_SPEED_1'])):
        if i == 0: continue

        before_time = total_dict['DRIVE_SPEED_1'][i-1][0]
        current_time = total_dict['DRIVE_SPEED_1'][i][0]
        
        if int(total_dict['DRIVE_LENGTH_TOTAL_1'][current_time]) - int(total_dict['DRIVE_LENGTH_TOTAL_1'][before_time]) < 2:
            if int(current_time) - int(before_time) > 60:
                if int(total_dict['DRIVE_SPEED_1'][i][1]) == 0 and int(total_dict['DRIVE_SPEED_1'][i-1][1]) == 0:
                
                    for timestamp in range(int(before_time)+1, int(current_time)-1):
                        put_dps_dict[str(timestamp)] = 1

                        
    copied_ori_dict['dps'] = put_dps_dict
    copied_ori_dict['tags']['work'] = 'parking'
    new_data_list.append(copied_ori_dict)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list
        
def _parking_v2(s_ut, e_ut, _dictbuf_list, _sec, meta):
    length_diff = 1

    total_dict = dict()
    put_dps_dict = dict()

    new_data_list = []

    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_LENGTH_TOTAL_1':
            copied_ori_dict = copy.deepcopy(_dict)
            sorted_spd = sorted(_dict['dps'].items())
            total_dict[_dict['tags']['fieldname']] = sorted_spd
        else:
            total_dict[_dict['tags']['fieldname']] = _dict['dps']

    for i in range(len(total_dict['DRIVE_LENGTH_TOTAL_1'])):
        if i == 0: continue

        before_time = total_dict['DRIVE_LENGTH_TOTAL_1'][i-1][0]
        current_time = total_dict['DRIVE_LENGTH_TOTAL_1'][i][0]
        
        if int(total_dict['DRIVE_LENGTH_TOTAL_1'][i-1][1]) - int(total_dict['DRIVE_LENGTH_TOTAL_1'][i-1][1]) < length_diff:
            if int(current_time) - int(before_time) > 7200:
                if int(total_dict['DRIVE_SPEED_1'][before_time]) <= 5 and int(total_dict['DRIVE_SPEED_1'][current_time]) <= 5:
                
                    for timestamp in range(int(before_time)+1, int(current_time)-1):
                        put_dps_dict[str(timestamp)] = 1

    copied_ori_dict['dps'] = put_dps_dict
    copied_ori_dict['tags']['work'] = 'parking'
    new_data_list.append(copied_ori_dict)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list


if __name__ == "__main__" : 

    print ("working good")