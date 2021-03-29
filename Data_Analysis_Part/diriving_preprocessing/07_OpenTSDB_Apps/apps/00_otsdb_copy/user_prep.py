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
    ret = _copy(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

def _copy(s_ut, e_ut, _dictbuf_list, _sec, meta):
    if meta['out_metric'] == 'none':
        return []
    else:
        return _dictbuf_list

def _copy_with_zero_value(s_ut, e_ut, _dictbuf_list, _sec, meta):
    put_dps_dict = dict()

    new_data_list = []

    for _dict in _dictbuf_list:
        copied_ori_dict = copy.deepcopy(_dict)
        sorted_timestamp = sorted(_dict['dps'].keys())
        
    for i in range(len(sorted_timestamp)):
        put_dps_dict[str(sorted_timestamp[i])] = 0

    copied_ori_dict['dps'] = put_dps_dict
    copied_ori_dict['tags']['fieldname'] = 'DRIVE_SPEED_1'
    copied_ori_dict['tags']['work'] = 'origin_and_parking'
    new_data_list.append(copied_ori_dict)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list
        
if __name__ == "__main__" : 

    print ("working good")