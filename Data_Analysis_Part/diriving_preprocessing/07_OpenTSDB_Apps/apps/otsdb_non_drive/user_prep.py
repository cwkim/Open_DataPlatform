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
    ret = _non_drive(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

# hanuri_tien의 주차구간을 구해줌
def _non_drive(s_ut, e_ut, _dictbuf_list, _sec, meta):
    length_diff = 1

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

    firstZero=-1
    lastZero=-1
    for i in range(len(total_dict['DRIVE_SPEED_1'])):
        # 2시간 이상 속도가 0일경우 주차구간으로 판단
        if total_dict['DRIVE_SPEED_1'][i][1] == 0:
            if firstZero == -1:
                firstZero = i
        elif total_dict['DRIVE_SPEED_1'][i][1] != 0 or i == len(total_dict['DRIVE_SPEED_1'])-1:
            if firstZero != -1:
                lastZero = i
                if i != len(total_dict['DRIVE_SPEED_1'])-1:
                    lastZero -= 1
                startTime = int(total_dict['DRIVE_SPEED_1'][firstZero][0])
                endTime = int(total_dict['DRIVE_SPEED_1'][lastZero][0])
                if endTime-startTime >= 3600*2 :
                    for timestamp in range(startTime, endTime):
                        put_dps_dict[str(timestamp)] = 0
                firstZero=-1
                lastZero=-1

        # 2시간 이상 속도 데이터가 존재하지 않고 주행거리가 변화없을시 주차구간으로 판단
        if i == 0: continue
        before_time = total_dict['DRIVE_SPEED_1'][i-1][0]
        current_time = total_dict['DRIVE_SPEED_1'][i][0]
        before_length=-2
        current_length=-1
        add=0
        while True:
            if add==300:
                break
            try:
                before_length=total_dict['DRIVE_LENGTH_TOTAL_1'][str(int(before_time)+add)]
                break
            except KeyError:
                add+=1
        add=0
        while True:
            if add==300:
                break
            try:
                current_length=total_dict['DRIVE_LENGTH_TOTAL_1'][str(int(current_time)-add)]
                break
            except KeyError:
                add+=1
        if int(current_length) - int(before_length) < length_diff:
            if int(current_time) - int(before_time) > 7200:
                for timestamp in range(int(before_time)+1, int(current_time)-1):
                    put_dps_dict[str(timestamp)] = 0

    copied_ori_dict['dps'] = put_dps_dict
    copied_ori_dict['tags']['work'] = 'parking'
    new_data_list.append(copied_ori_dict)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list


if __name__ == "__main__" : 
    print ("working good")
