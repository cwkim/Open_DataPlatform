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
    ret = get_startend_by_car_status(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

def get_startend_by_car_status(s_ut, e_ut, _dictbuf_list, _sec, meta):
    total_dict = dict()
    put_dps_dict1 = dict()
    put_dps_dict2 = dict()
    new_data_list = []
    copied_ori_dict={}
    time_diff=7200

    for _dict in _dictbuf_list:
        if _dict['metric']=='Elex_06_sample':
            if _dict['tags']['fieldname'] == 'DRIVE_LENGTH_TOTAL_1':
                total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['metric']=='Elex_06_status_v2':    
            if _dict['tags']['work'] == 'driving':
                total_dict[_dict['tags']['work']] = _dict['dps']
                copied_ori_dict = copy.deepcopy(_dict)
        
    total_dict['driving'] = sorted(total_dict['driving'].items())
    i=0
    start=-1
    end=-1
    # time_diff시간을 기준으로 주행구간 분리
    while True:
        if i+1 >= len(total_dict['driving']):
            break
        if start == -1:
            start = i
        if int(total_dict['driving'][i+1][0])-int(total_dict['driving'][i][0]) > time_diff or i+1 == len(total_dict['driving'])-1:
            end = i
            if i+1 == len(total_dict['driving'])-1:
                end = i+1
            startts = total_dict['driving'][start][0]
            endts = total_dict['driving'][end][0]
            # 10분 이상 주행한 경우를 유효한 주행 구간으로 지정
            if int(endts) - int(startts) >= 600:
                add=0
                start_dis=0
                end_dis=0
                while True: # startts, endts가 DRIVE_LENGTH_TOTAL_1 dict에 없을경우 가까운 값 탐색
                    try:
                        if int(startts)+add >= int(endts):
                            break
                        start_dis = total_dict['DRIVE_LENGTH_TOTAL_1'][str(int(startts)+add)]
                        break
                    except KeyError:
                        add+=1
                add=0
                while True:
                    try:
                        if int(startts) >= int(endts)+add:
                            break
                        end_dis = total_dict['DRIVE_LENGTH_TOTAL_1'][str(int(endts)+add)]
                        break
                    except KeyError:
                        add-=1
                # 5km 이상 주행할 경우 주행으로 판단
                if end_dis - start_dis > 5 and start_dis != 0 and end_dis != 0:
                    put_dps_dict1[startts] = 0
                    put_dps_dict2[endts] = 1
            start=-1
            end=-1
        i+=1
    
    del copied_ori_dict['tags']['work']
    copied_ori_dict['tags']['fieldname'] = 'DRIVE_START'
    copied_ori_dict['dps']=put_dps_dict1
    new_data_list.append(copied_ori_dict)
    copied_ori_dict = copy.deepcopy(copied_ori_dict)
    copied_ori_dict['tags']['fieldname'] = 'DRIVE_END'
    copied_ori_dict['dps']=put_dps_dict2
    new_data_list.append(copied_ori_dict)
    
    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list
        
if __name__ == "__main__" : 

    print ("working good")