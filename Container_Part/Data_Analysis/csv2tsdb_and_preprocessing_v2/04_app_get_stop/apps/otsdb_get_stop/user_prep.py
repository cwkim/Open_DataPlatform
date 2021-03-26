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
    ret = _getstop(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret
        
def _getstop(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}
    put_dps_dict1={}
    put_dps_dict2={}
    
    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_START':
            copied_ori_dict = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'DRIVE_END':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'DRIVE_SPEED':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'DRIVE_LENGTH_TOTAL':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
    
    #각 주행 구간의 시작 지점, 종료 지점 list로 저장
    endkeys = total_dict['DRIVE_END'].keys()
    endkeys.sort()
    startkeys = total_dict['DRIVE_START'].keys()
    startkeys.sort()
    DS_key_list = list(total_dict['DRIVE_SPEED'].keys())
    DS_key_list.sort()

    i=0
    #주행 구간 탐색
    for k in range(len(endkeys)):
        start = startkeys[k]
        end = endkeys[k]

        while i < len(DS_key_list) and DS_key_list[i] < start:
            i+=1

        zero_start = -1
        zero_end = -1
        while i < len(DS_key_list) and DS_key_list[i] < end:
            if total_dict['DRIVE_SPEED'][DS_key_list[i]] == 0 and zero_start == -1:
                zero_start = DS_key_list[i]
            elif (zero_start != -1 and total_dict['DRIVE_SPEED'][DS_key_list[i]] != 0) or (zero_start != -1 and (DS_key_list[i+1] >= end or i+1 == len(DS_key_list))):
                if total_dict['DRIVE_SPEED'][DS_key_list[i]] != 0:
                    zero_end = DS_key_list[i-1]
                else:
                    zero_end = DS_key_list[i]
                zero_start = int(zero_start)
                zero_end = int(zero_end)
                if zero_end - zero_start >= 480:
                    #누적주행거리 차가 1보다 작은 경우 추가
                    
                    end_drive_length = -1
                    start_drive_length = -1
                    add = 0
                    while add < (zero_end-zero_start)/2:
                        try:
                            end_drive_length = total_dict['DRIVE_LENGTH_TOTAL'][str(zero_end-add)]
                            break
                        except KeyError:
                            add+=1
                    add = 0
                    while add < (zero_end-zero_start)/2:
                        try:
                            start_drive_length = total_dict['DRIVE_LENGTH_TOTAL'][str(zero_start+add)]
                            break
                        except KeyError:
                            add+=1
                    # speed로 구한 정차 구간의 값을 1로 변경
                    if end_drive_length - start_drive_length <= 1 and start_drive_length != -1 and end_drive_length != -1:
                        #print(zero_start, zero_end)
                        data_cnt=0
                        # 데이터 손실에 의해 정차구간으로 지정되는 경우 배제
                        for stop_ts in range(zero_start, zero_end+1):
                            try:
                                total_dict['DRIVE_SPEED'][str(stop_ts)]
                                data_cnt+=1
                            except KeyError:
                                continue
                        if data_cnt / float(zero_end+1-zero_start) * 100 > 90:
                            put_dps_dict1[str(zero_start)] = 0
                            put_dps_dict2[str(zero_end)] = 1

                zero_start = -1
                zero_end = -1
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
