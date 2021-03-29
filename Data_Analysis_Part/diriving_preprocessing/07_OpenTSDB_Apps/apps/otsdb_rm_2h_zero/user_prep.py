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
    ret = _driving(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret
        
def _driving(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}
    put_dps_dict = {}

    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_SPEED_1':
            copied_ori_dict = copy.deepcopy(_dict)
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
                # 모든 차량에서 약 2시간반 가량의 데이터가 skip된 부분 제외해줌
                if startTime < 1559595600 and 1559595600 < endTime:
                    if endTime-startTime-(7200+1800) >= 3600*2 :
                        for keyidx in range(firstZero, lastZero):
                            while True:
                                try:
                                    del total_dict['DRIVE_SPEED_1'][DSkeylist[keyidx]]
                                except KeyError:
                                    break
                else:
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
        
    copied_ori_dict['dps'] = total_dict['DRIVE_SPEED_1']
    copied_ori_dict['tags']['work'] = 'driving'
    new_data_list.append(copied_ori_dict)
    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")
