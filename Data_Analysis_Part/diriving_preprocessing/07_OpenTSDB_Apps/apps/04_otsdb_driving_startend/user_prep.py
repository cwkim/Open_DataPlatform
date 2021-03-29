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
        if _dict['tags']['fieldname'] == 'DRIVE_SPEED_1':
            copied_ori_dict1 = copy.deepcopy(_dict)
            copied_ori_dict2 = copy.deepcopy(_dict)
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        else :
            total_dict[_dict['tags']['fieldname']] = _dict['dps']

    DSkeylist = total_dict['DRIVE_SPEED_1'].keys()
    DSkeylist.sort()

    
    firstZero=-1
    lastZero=-1
    idx=0
    for ts in DSkeylist:
        if total_dict['DRIVE_SPEED_1'][ts] == 0:
            if firstZero == -1:
                firstZero = idx      
        elif total_dict['DRIVE_SPEED_1'][ts] != 0 or idx == len(total_dict['DRIVE_SPEED_1'])-1:
            if firstZero != -1:
                lastZero = idx
                if idx != len(total_dict['DRIVE_SPEED_1'])-1:
                    lastZero -= 1
                startTime = int(DSkeylist[firstZero])
                endTime = int(DSkeylist[lastZero])
                
                if endTime-startTime >= 3600*2 :
                    for keyidx in range(firstZero, lastZero):
                        while True:
                            try:
                                del total_dict['DRIVE_SPEED_1'][DSkeylist[keyidx]]
                            except KeyError:
                                break
                firstZero=-1
                lastZero=-1
        idx+=1

    DSkeylist = total_dict['DRIVE_SPEED_1'].keys()
    DSkeylist.sort()

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
                while True: # DRIVE_SPEED_1의 timestamp가 DRIVE_LENGTH_TOTAL_1에 없을경우 가까운 값 탐색
                    try:
                        if add > time_diff:
                            break
                        start_dis = total_dict['DRIVE_LENGTH_TOTAL_1'][str(start+add)]
                        break
                    except KeyError:
                        add+=1
                add=0
                while True:
                    try:
                        if add < -time_diff:
                            break
                        end_dis = total_dict['DRIVE_LENGTH_TOTAL_1'][str(end+add)]
                        break
                    except KeyError:
                        add-=1
                # 5km 이상 주행할 경우 주행으로 판단
                if end_dis - start_dis > 5 and start_dis != 0 and end_dis != 0:
                    put_dps_dict1[str(start)] = 0
                    put_dps_dict2[str(end)] = 1
            start=-1
            end=-1

    
    copied_ori_dict1['dps'] = put_dps_dict1
    copied_ori_dict1['tags']['drive'] = 'start'
    new_data_list.append(copied_ori_dict1)
    copied_ori_dict2['dps'] = put_dps_dict2
    copied_ori_dict2['tags']['drive'] = 'end'
    new_data_list.append(copied_ori_dict2)
    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")
