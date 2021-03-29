# -*- coding: utf-8 -*-
#!/usr/bin/env python2

from __future__ import print_function
import os
import sys
import time
import datetime
from collections import OrderedDict
import copy
import pandas as pd

# 호출되는 함수 
def prep(s_ut, e_ut, _dictbuf_list, _sec, meta):
    ret = average_driving_length_and_time(s_ut, e_ut, _dictbuf_list, _sec, meta)
    return ret


def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[11:13], _time[14:16], _time[17:])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))
    return epoch


def unixtime2datetime(unixtime):
    _date_time = datetime.datetime.fromtimestamp(int(unixtime))
    _date = datetime.datetime.strftime(_date_time, '%Y/%m/%d-%H:%M:%S')

    return _date


def make_df(_querydata):
    carid=None
    total_dict={}
    for _dict in _querydata:
        carid = _dict['tags']['VEHICLE_NUM']
        if _dict['tags']['fieldname'] == 'DRIVE_TIME':
            total_dict[_dict['tags']['fieldname']] = sorted(_dict['dps'].items())
        else :
            total_dict[_dict['tags']['fieldname']] = sorted(_dict['dps'].items())

    caridlist=[]
    drvietime=[]
    drivelength=[]
    lengthperhour=[]    
    for i in range(len(total_dict['DRIVE_TIME'])):
        caridlist.append(carid)
        drvietime.append(int(total_dict['DRIVE_TIME'][i][1]))
        drivelength.append(total_dict['DRIVE_LENGTH'][i][1])
        lperh = float(total_dict['DRIVE_LENGTH'][i][1]) / int(total_dict['DRIVE_TIME'][i][1]) * 3600
        lengthperhour.append(lperh)
    df = pd.DataFrame({'VEHICLE_NUM':caridlist, 'DRIVE_TIME':drvietime, 'DRIVE_LENGTH':drivelength, 'LENGTH_PER_HOUR':lengthperhour})
    return df


def average_driving_length_and_time(s_ut, e_ut, _dictbuf_list, _sec, meta):
    new_data_list = []
    dflist = []
    new_list = []
    id_list = []
    for _dict in _dictbuf_list:
        id_list.append(str(_dict['tags']['VEHICLE_NUM']))
    id_list = list(set(id_list))

    for _id in id_list:
        for _dict in _dictbuf_list:
            if str(_dict['tags']['VEHICLE_NUM']) == str(_id):
                new_list.append(_dict)

        new_df = make_df(new_list)
        dflist.append(new_df)
    
    finaldf = dflist[0]
    for i in range(1,len(dflist)):
        finaldf = pd.concat([finaldf, dflist[i]])
    finaldf = finaldf.reset_index()
    del finaldf['index']

    mean = list(finaldf.mean(axis=0))
    mean[3] = 'NaN'
    finaldf = pd.concat([finaldf, pd.DataFrame({'VEHICLE_NUM':[mean[3]], 'DRIVE_TIME':[mean[1]], 'DRIVE_LENGTH':[mean[0]], 'LENGTH_PER_HOUR':[mean[2]]})])
    finaldf = finaldf.reset_index()
    del finaldf['index']

    if meta['carid'] == '*':
        tags_id = 'total'
    else:
        tags_id = id_list[0]

    put_dps_dict1 = {s_ut : round(mean[1], 2)}
    put_dps_dict2 = {s_ut : round(mean[0], 2)}
    put_dps_dict3 = {s_ut : round(mean[2], 2)}

    tags_dict1 = { 'VEHICLE_NUM' : str(tags_id), 'fieldname' : 'DRIVE_TIME' }
    tags_dict2 = { 'VEHICLE_NUM' : str(tags_id), 'fieldname' : 'DRIVE_LENGTH' }
    tags_dict3 = { 'VEHICLE_NUM' : str(tags_id), 'fieldname' : 'LENGTH_PER_HOUR' }
    copied_ori_dict = copy.deepcopy(_dictbuf_list[0])
    copied_ori_dict['dps'] = put_dps_dict1
    copied_ori_dict['tags'] = tags_dict1
    new_data_list.append(copied_ori_dict)
    copied_ori_dict = copy.deepcopy(_dictbuf_list[0])
    copied_ori_dict['dps'] = put_dps_dict2
    copied_ori_dict['tags'] = tags_dict2
    new_data_list.append(copied_ori_dict)
    copied_ori_dict = copy.deepcopy(_dictbuf_list[0])
    copied_ori_dict['dps'] = put_dps_dict3
    copied_ori_dict['tags'] = tags_dict3
    new_data_list.append(copied_ori_dict)

    if meta['out_metric'] == 'none':
        return []
    else:
        return new_data_list

if __name__ == "__main__" : 
    print ("working good")