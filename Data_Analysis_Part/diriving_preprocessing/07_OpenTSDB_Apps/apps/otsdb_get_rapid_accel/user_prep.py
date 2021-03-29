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
    ret = _check_accel_decel(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

#UNIX TIME 을 date 형식으로 변환
def convert_datetime(unixtime):
    """Convert unixtime to datetime"""
    import datetime
    date = datetime.datetime.fromtimestamp(unixtime).strftime('%Y-%m-%d %H:%M:%S')
    return date # format : str


def _check_accel_decel(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}

    for _dict in _dictbuf_list:
        if _dict['metric'] == 'Elex.2020.01.data_full_v2':
            if _dict['tags']['fieldname'] == 'DRIVE_SPEED_1':
                copied_ori_dict1 = copy.deepcopy(_dict)
                copied_ori_dict2 = copy.deepcopy(_dict)
                total_dict[_dict['tags']['fieldname']] = _dict['dps']
        else:
            total_dict[_dict['tags']['drive']] = _dict['dps']

    endkeys = total_dict['end'].keys()
    endkeys.sort()

    startkeys = total_dict['start'].keys()
    startkeys.sort()
    status='none'
             #급감속, 급정지, 급가속, 급출발
    st_list=['DECEL','SSTOP','ACCEL','QSTART','none']

    #status의 구간들이 저장될 디렉터리
    st_dict={}

    #initialize
    for st in st_list:
        st_dict[st]=[]

    #주행구간 loop
    for idx in range(len(endkeys)):
        start = int(startkeys[idx])
        end = int(endkeys[idx])
        i=start
        print("***************************주행구간 %d번째 : %s ~ %s *****************************" %(idx,convert_datetime(start),convert_datetime(start)))
        while i <= end:
            cur_spd=int(total_dict['DRIVE_SPEED_1'][str(i)])
            j=i+1
            while j <= end:
                next_spd = int(total_dict['DRIVE_SPEED_1'][str(j)])
                diff = (next_spd - cur_spd) / (j - i)
                if status == 'none':
                    if diff <= -8:
                        # 급 감속 추출
                        if next_spd >= 6:
                            status = 'DECEL'
                            j += 1
                            continue
                        # 급 정지 추출
                        else:
                            status = 'SSTOP'
                            j += 1
                            continue
                    # 급 가속 유형
                    elif diff >= 5:
                        # 급 가속 추출
                        if total_dict['DRIVE_SPEED_1'][str(j-1)] >= 6:
                            status = 'ACCEL'
                            j += 1
                            continue
                        # 급 출발 추출
                        elif diff >= 6:
                            status = 'QSTART'
                            j += 1
                            continue
                    break

                elif status == 'DECEL':
                    if diff <= -8:
                        if next_spd >=6:
                            j+=1
                            continue
                    break
                elif status == 'SSTOP':
                    if diff <= -8:
                        if next_spd <6:
                            j+=1
                            continue
                    break
                elif status == 'ACCEL':
                    if diff >= 5:
                        if total_dict['DRIVE_SPEED_1'][str(j-1)] >= 6:
                            j+=1
                            continue
                    break
                elif status == 'QSTART':
                    if diff >= 6:
                        if total_dict['DRIVE_SPEED_1'][str(j-1)] < 6:
                            j+=1
                            continue
                    break

            #status 유형과 start지점 i, end지점 j 추출
            if status != 'none' and i+1 != j:
                print("status : %s 구간 : %s ~ %s" %(status,convert_datetime(i),convert_datetime(j-1)))
                st_dict[status].append([str(i),str(j-1)])

            i=j
            status='none'

    del copied_ori_dict1['tags']['work']
    del copied_ori_dict2['tags']['work']

    #status 별로 탐색
    for i,key in enumerate(list(st_dict.keys())):
        if key != 'none':
            #구간의 시작, 종료 지점이 저장된 list
            tmp_li = st_dict[key]

            put_dps_dict1 = {}
            put_dps_dict2 = {}
            for _li in tmp_li:
                put_dps_dict1[_li[0]] = i*10 - 1
                put_dps_dict2[_li[1]] = i*10 + 1

            #status 유형과 시작 point data_list에 append
            copied_ori_dict1['dps'] = copy.deepcopy(put_dps_dict1)
            copied_ori_dict1['tags']['fieldname'] = 'start2'
            copied_ori_dict1['tags']['status'] = key
            new_data_list.append(copy.deepcopy(copied_ori_dict1))

            #status 유형과 종료 point data_list에 append
            copied_ori_dict2['dps'] = copy.deepcopy(put_dps_dict2)
            copied_ori_dict2['tags']['fieldname'] = 'end2'
            copied_ori_dict2['tags']['status'] = key
            new_data_list.append(copy.deepcopy(copied_ori_dict2))
    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")
