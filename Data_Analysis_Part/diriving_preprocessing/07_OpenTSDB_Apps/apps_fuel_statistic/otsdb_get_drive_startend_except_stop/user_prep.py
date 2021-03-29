# -*- coding: utf-8 -*-
#!/usr/bin/env python2

from __future__ import print_function
import os
import sys
import time
import datetime
from collections import OrderedDict
import copy
from haversine import haversine
# 호출되는 함수 
def prep(s_ut, e_ut, _dictbuf_list, _sec, meta):
    ret = _get_drive_except_stop(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret
        
def _get_drive_except_stop(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}
    put_dps_dict3={} # 정차구간에 의해 분리된 주행구간 시작
    put_dps_dict4={} # 정차구간에 의해 분리된 주행구간 시작

    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'STOP_START':
            copied_ori_dict3 = copy.deepcopy(_dict)
            copied_ori_dict4 = copy.deepcopy(_dict)
            copied_ori_dict5 = copy.deepcopy(_dict)
        total_dict[_dict['tags']['fieldname']] = _dict['dps']


    #각 주행 구간의 시작 지점, 종료 지점 list로 저장
    endkeys = list(total_dict['DRIVE_END'].keys())
    endkeys.sort()
    startkeys = list(total_dict['DRIVE_START'].keys())
    startkeys.sort()

    #각 휴식 구간 시작 지점, 종료 지점 list로 저장
    rest_endkeys = list(total_dict['STOP_END'].keys())
    rest_endkeys.sort()
    rest_startkeys = list(total_dict['STOP_START'].keys())
    rest_startkeys.sort()

    i=0 # 주행 구간 idx
    j=0 # 휴식 구간 idx
    while i < len(endkeys):
        # 정차구간에 의해 분리된 주행구간의 시작,끝
        put_start = startkeys[i] 
        put_end=None

        while j < len(rest_endkeys) and int(rest_endkeys[j]) < int(endkeys[i]):
            put_end=str(int(rest_startkeys[j])-1)
            put_dps_dict3[put_start] = 3
            put_dps_dict4[put_end] = 4

            put_start = str(int(rest_endkeys[j])+1)
            j+=1
        put_end = endkeys[i]

        put_dps_dict3[put_start] = 3
        put_dps_dict4[put_end] = 4

        i+=1


    copied_ori_dict3['dps'] = put_dps_dict3
    copied_ori_dict3['tags']['fieldname'] = 'DRIVE_START'
    new_data_list.append(copied_ori_dict3)

    copied_ori_dict4['dps'] = put_dps_dict4
    copied_ori_dict4['tags']['fieldname'] = 'DRIVE_END'
    new_data_list.append(copied_ori_dict4)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    None


