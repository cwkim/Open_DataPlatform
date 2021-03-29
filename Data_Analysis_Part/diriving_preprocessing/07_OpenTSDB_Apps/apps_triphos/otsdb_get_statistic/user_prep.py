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
    ret = func(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

def func(s_ut, e_ut, _dictbuf_list, _sec, meta):
    total_dict = dict()
    new_data_list = []

    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_SPEED':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'CAR_STATUS':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'RPM':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']

    DPS_ts_list = list(total_dict['CAR_STATUS'].keys())
    DPS_ts_list.sort()

    td=0
    d=0
    ts=0
    s=0
    tp=0
    p=0

    for _ts in DPS_ts_list:
        val = total_dict['CAR_STATUS'][_ts]
        if val == 2:
            td+=1
            try:
                if total_dict['DRIVE_SPEED'][_ts] > 0 and total_dict['RPM'][_ts] > 0:
                    d+=1
            except KeyError:
                None
        elif val == 1:
            ts+=1
            try:
                if total_dict['DRIVE_SPEED'][_ts] == 0 and total_dict['RPM'][_ts] > 0:
                    s+=1
            except KeyError:
                None
        elif val == 0:
            tp+=1
            try:
                if total_dict['DRIVE_SPEED'][_ts] == 0 and total_dict['RPM'][_ts] == 0:
                    p+=1
            except KeyError:
                None

    print('D')
    print('total : ' + str(td) + ' / ' + str(d))
    print('percent : ' + str(float(d)/td*100))
    
    print('S')
    print('total : ' + str(ts) + ' / ' + str(s))
    print('percent : ' + str(float(s)/ts*100))
    
    print('P')
    print('total : ' + str(tp) + ' / ' + str(p))
    print('percent : ' + str(float(p)/tp*100))
    print('\n')

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list
        
if __name__ == "__main__" : 

    print ("working good")