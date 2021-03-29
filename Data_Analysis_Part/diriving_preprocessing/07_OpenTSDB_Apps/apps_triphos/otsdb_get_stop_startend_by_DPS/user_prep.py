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
    ret = _getse(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret
        
def _getse(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}
    put_dps_dict1 = {}
    put_dps_dict2 = {}
    
    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_START':
            copied_ori_dict = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'DRIVE_END':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'CAR_STATUS':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
    

    start_ts_list = list(total_dict['DRIVE_START'].keys())
    start_ts_list.sort()
    end_ts_list = list(total_dict['DRIVE_END'].keys())
    end_ts_list.sort()
    DPS_ts_list = list(total_dict['CAR_STATUS'].keys())
    DPS_ts_list.sort()
    

    i=0
    j=0
    while i < len(start_ts_list):
        start_ts = start_ts_list[i]
        end_ts = end_ts_list[i]
        
        while DPS_ts_list[j] < start_ts:
            j+=1
            
        stop_start_ts=-1
        stop_end_ts=-1
        while j < len(DPS_ts_list) and DPS_ts_list[j] <= end_ts:
            val = total_dict['CAR_STATUS'][DPS_ts_list[j]]
            if stop_start_ts == -1 and val == 1:
                stop_start_ts = DPS_ts_list[j]
            elif stop_start_ts != -1 and (val != 1 or DPS_ts_list[j] == end_ts):
                if DPS_ts_list[j] == end_ts and val == 1:
                    stop_end_ts = DPS_ts_list[j]
                else:
                    stop_end_ts = DPS_ts_list[j-1]
                
                if int(stop_end_ts) - int(stop_start_ts) >= 480:
                    data_cnt=0
                    # 데이터 손실에 의해 정차구간으로 지정되는 경우 배제
                    for stop_ts in range(int(stop_start_ts), int(stop_end_ts)+1):
                        try:
                            total_dict['CAR_STATUS'][str(stop_ts)]
                            data_cnt+=1
                        except KeyError:
                            continue
                    if data_cnt / float(int(stop_end_ts)+1-int(stop_start_ts)) * 100 > 90:
                        put_dps_dict1[stop_start_ts] = 0
                        put_dps_dict2[stop_end_ts] = 1

                stop_start_ts = -1
                stop_end_ts = -1 
            j+=1

        i+=1
    
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
