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
        if _dict['tags']['fieldname'] == 'DRIVE_SPEED':
            copied_ori_dict1 = copy.deepcopy(_dict)
            copied_ori_dict2 = copy.deepcopy(_dict)
            copied_ori_dict3 = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        else :
            total_dict[_dict['tags']['fieldname']] = _dict['dps']

    DSkeylist = list(total_dict['DRIVE_SPEED'].keys())
    DSkeylist.sort()
    print(len(DSkeylist))
    start=-1
    end=-1
    for i in range(len(DSkeylist)-1):
        if start==-1:
            start = int(DSkeylist[i])
        # 데이터가 분리된 기간이 2시간 이상일 경우
        if int(DSkeylist[i+1]) - int(DSkeylist[i]) >= 7200 or i+1 == len(DSkeylist)-1:
            if i+1 == len(DSkeylist)-1:
                end = int(DSkeylist[i+1])
            else:    
                end = int(DSkeylist[i])
            # 10분 이상 주행할 경우 주행으로 판단
            if end-start >= 600:
                add=0
                start_dis=0
                end_dis=0
                time_diff = (end-start)/2
                _check1=False
                _check2=False
                while True: # DRIVE_SPEED_1의 timestamp가 DRIVE_LENGTH_TOTAL_1에 없을경우 가까운 값 탐색
                    try:
                        if add > time_diff:
                            _check1=True
                            break
                        start_dis = total_dict['DRIVE_LENGTH_TOTAL'][str(start+add)]
                        break
                    except KeyError:
                        add+=1
                add=0
                while True:
                    try:
                        if add < -time_diff:
                            _check2=True
                            break
                        end_dis = total_dict['DRIVE_LENGTH_TOTAL'][str(end+add)]
                        break
                    except KeyError:
                        add-=1
                if _check1 or _check2:
                    start=-1
                    end=-1
                    continue
                # 5km 이상 주행할 경우 주행으로 판단
                if end_dis - start_dis > 5 and start_dis != 0 and end_dis != 0:
                    put_dps_dict1[str(start)] = 0
                    put_dps_dict2[str(end)] = 1
            start=-1
            end=-1

    
    copied_ori_dict1['dps'] = put_dps_dict1
    copied_ori_dict1['tags']['fieldname'] = 'DRIVE_START'
    new_data_list.append(copied_ori_dict1)
    copied_ori_dict2['dps'] = put_dps_dict2
    copied_ori_dict2['tags']['fieldname'] = 'DRIVE_END'
    new_data_list.append(copied_ori_dict2)
    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")
