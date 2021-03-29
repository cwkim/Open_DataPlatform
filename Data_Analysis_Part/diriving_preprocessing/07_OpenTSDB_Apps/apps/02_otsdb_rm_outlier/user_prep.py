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
    ret = _rm_outlier(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

def _rm_outlier(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}
    put_dps_dict = {}
    put_dps_dict2 = {}
    
    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_LENGTH_TOTAL':
            copied_ori_dict1 = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = sorted(_dict['dps'].items())
        elif _dict['tags']['fieldname'] == 'DRIVE_SPEED':
            copied_ori_dict2 = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = sorted(_dict['dps'].items())

    # 누적 주행 거리 데이터 이상치 제거
    cons=1 # 누적주행거리가 km단위면 cons=1, meter단위면 cons=1000
    nowidx=0
    nextidx=nowidx+1
    while True:
        if nowidx >= len(total_dict["DRIVE_LENGTH_TOTAL"]):
            break
        if total_dict['DRIVE_LENGTH_TOTAL'][nowidx][1] > 0:
            break
        del total_dict['DRIVE_LENGTH_TOTAL'][nowidx]
    if nowidx < len(total_dict["DRIVE_LENGTH_TOTAL"]):
        while True:
            nowval = total_dict['DRIVE_LENGTH_TOTAL'][nowidx][1]
            nowval/=cons
            if nextidx >= len(total_dict["DRIVE_LENGTH_TOTAL"]):
                put_dps_dict[total_dict['DRIVE_LENGTH_TOTAL'][nowidx][0]] = nowval
                break
            nextval = total_dict['DRIVE_LENGTH_TOTAL'][nextidx][1]
            nextval/=cons
            # 1초당 이동할 수 있는 최대 거리 0.073km (시속 260km 기준)
            # 최소 14초가 지나야 1km이상 이동 가능
            # 하지만 실제 데이터 값은 정수형이므로 13초 사이에 2km이상(1km초과) 이동하면 이상치로 판단
            tsdif = int(total_dict['DRIVE_LENGTH_TOTAL'][nextidx][0])- int(total_dict['DRIVE_LENGTH_TOTAL'][nowidx][0])        
            if nextval-nowval > 1 * int(tsdif/14) + 2 or nextval-nowval < 0:
                del total_dict['DRIVE_LENGTH_TOTAL'][nextidx]
                continue
            put_dps_dict[total_dict['DRIVE_LENGTH_TOTAL'][nowidx][0]] = nowval
            nowidx = nextidx
            nextidx = nowidx+1

    # 속도 데이터 이상치 제거
    for i in range(len(total_dict['DRIVE_SPEED'])):
        val = total_dict['DRIVE_SPEED'][i][1]
        if val < 0 or val > 260:
            continue
        put_dps_dict2[total_dict['DRIVE_SPEED'][i][0]] = val
    
    copied_ori_dict1['dps'] = put_dps_dict
    copied_ori_dict1['tags']['fieldname'] = 'DRIVE_LENGTH_TOTAL_1'
    new_data_list.append(copied_ori_dict1)
    copied_ori_dict2['dps'] = put_dps_dict2
    copied_ori_dict2['tags']['fieldname'] = 'DRIVE_SPEED_1'
    new_data_list.append(copied_ori_dict2)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")
