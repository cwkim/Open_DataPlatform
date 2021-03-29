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
    ret = _get_fuel(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret

def convertEpochtoTimestr(_epoch):
    d = _epoch / (3600*24)
    _epoch -= d*3600*24
    h = _epoch / 3600
    _epoch -= h*3600
    m = _epoch / 60
    _epoch -= m*60
    s = _epoch
    return '%03d-%02d:%02d:%02d' %(d,h,m,s)
        
def _get_fuel(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list=[]
    total_dict={}
    carid=0
    for _dict in _dictbuf_list:
        if _dict['tags']['fieldname'] == 'DRIVE_LENGTH_TOTAL_1':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        elif _dict['tags']['fieldname'] == 'FUEL_CONSUM_TOTAL':
            total_dict[_dict['tags']['fieldname']] = _dict['dps']
        if _dict['metric'] == 'Elex.2020.01.driving_startend_groupby_time_v2':   
            if _dict['tags']['SORT'] == '08_12':
                if _dict['tags']['fieldname'] == 'DRIVE_START':
                    total_dict[_dict['tags']['fieldname']] = _dict['dps']
                elif _dict['tags']['fieldname'] == 'DRIVE_END':
                    total_dict[_dict['tags']['fieldname']] = _dict['dps']
                    carid = _dict['tags']['VEHICLE_NUM']

    start_key_list = list(total_dict['DRIVE_START'].keys())
    start_key_list.sort()
    end_key_list = list(total_dict['DRIVE_END'].keys())
    end_key_list.sort()
    
    fuel_data_dict={}
    fuel_data_dict['TOTAL_TIME']=0
    fuel_data_dict['TOTAL_LENGTH']=0
    fuel_data_dict['FUEL_CONSUM']=0
    fuel_data_dict['FUEL_CONSUM_PER_TIME']=0
    fuel_data_dict['FUEL_EFFICIENCY']=0
    
    i=0
    while i < len(end_key_list):
        startts = start_key_list[i]
        endts = end_key_list[i]

        add=0
        start_length=-1
        while add < (int(endts)-int(startts))/2:
            try:
                start_length = total_dict['DRIVE_LENGTH_TOTAL_1'][str(int(startts)+add)]
                break
            except KeyError:
                add+=1
        add=0
        end_length=-1
        while add < (int(endts)-int(startts))/2:
            try:
                end_length = total_dict['DRIVE_LENGTH_TOTAL_1'][str(int(endts)+add)]
                break
            except KeyError:
                add-=1

        add=0
        start_fuel=-1
        while add < (int(endts)-int(startts))/2:
            try:
                start_fuel = total_dict['FUEL_CONSUM_TOTAL'][str(int(startts)+add)]
                break
            except KeyError:
                add+=1
        add=0
        end_fuel=-1
        while add < (int(endts)-int(startts))/2:
            try:
                end_fuel = total_dict['FUEL_CONSUM_TOTAL'][str(int(endts)+add)]
                break
            except KeyError:
                add-=1

        if end_fuel != -1 and start_fuel != -1 and start_length != -1 and end_length != -1:
            fuel_data_dict['TOTAL_TIME']+=(int(endts)-int(startts))
            fuel_data_dict['TOTAL_LENGTH']+=(end_length-start_length)
            fuel_data_dict['FUEL_CONSUM']+=float(end_fuel-start_fuel)/1000

        i+=1
    
    try:
        fuel_data_dict['FUEL_CONSUM_PER_TIME'] = fuel_data_dict['FUEL_CONSUM']/(fuel_data_dict['TOTAL_TIME']/3600)
    except ZeroDivisionError:
        fuel_data_dict['FUEL_CONSUM_PER_TIME'] = 0
    try:
        fuel_data_dict['FUEL_EFFICIENCY'] = fuel_data_dict['TOTAL_LENGTH']/fuel_data_dict['FUEL_CONSUM']
    except ZeroDivisionError:
        fuel_data_dict['FUEL_EFFICIENCY'] = 0
    fuel_data_dict['TOTAL_TIME'] = convertEpochtoTimestr(fuel_data_dict['TOTAL_TIME'])

    print(carid)
    for key in fuel_data_dict.keys():
        print(key + ' : ' + str(fuel_data_dict[key]))

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("%03d" %(1))
