#-*- coding:utf-8 -*-
'''
Author : CS Chae, github : https://github.com/ChulseoungChae
       : Jeonghoon Kang  : https://github.com/jeonghoonkang
'''

from __future__ import print_function
import pprint
import time
import pandas as pd
import os
import sys
import math
import json
import requests
import argparse
import Utils
from collections import OrderedDict


print_head = ' '*16 + '[LIB_OPENTSDB]'

""" 실제 작업에 필요한 query parameter 예 - 초기값 """

query_parameter = {
    "start" : "2014-06-01 00:00:00",
    "end": "2014-06-02 00:00:00",
    "aggregator" : "none",
    "metric" : "____test____"
}

""" 쿼리 하려는 tag """
query_tags = {
}

def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[-8:-6], _time[-5:-3], _time[-2:])
    #print date_time
    pattern = '%d.%m.%Y %H:%M:%S'
    epoch = int (time.mktime(time.strptime(date_time, pattern)))
    return epoch

def _processingResponse(in_data):
    '''openTSDB에서 전송받은, string 들을 dictionary로 변경하여
       dict 를 회신함. 형태를 변경하고, dict의 갯수를 알려줌'''
    _d = in_data
    _r = _d.json()
    # queryData.content is string, thus convert this to list
    _l = len(_r)

    return _r, _l

def QueryData(_url, _required, _tags=None):
    headers = {'content-type': 'application/json'}

    dp = OrderedDict()    # dp (Data Point)
    dp["start"] = convertTimeToEpoch(_required["start"])
    dp["end"] = convertTimeToEpoch(_required["end"]) - int(1)   # not exactly required

    temp = OrderedDict()
    temp["aggregator"] = _required["aggregator"]
    temp["metric"] = _required["metric"]
    if _tags != None:
        temp["tags"] = _tags

    dp["queries"] = []
    dp["queries"].append(temp)
    #print (print_head, json.dumps(dp))

    #print " [Querying]" + json.dumps(dp, ensure_ascii=False, indent=4)
    response = requests.post(_url, data=json.dumps(dp), headers= headers)

    while response.status_code > 204:
        print(print_head," [Bad Request] Query status: %s" % (response.status_code))
        print(print_head," [Bad Request] We got bad request, Query will be restarted after 3 sec!\n")
        time.sleep(3)

        print(print_head," [Querying]" + json.dumps(dp, ensure_ascii=False, indent=4))
        response = requests.post(_url, data=json.dumps(dp), headers= headers)

    pout = " [Query is done, got reponse from server]" + __file__
    pout += " : now starting processing, writing and more "
    #print(print_head,pout)
    return response

def query_by_timedelta_v3(_date, meta, dys=None, hrs=None, mins=None):
    global query_parameter
    
    if dys != None : 
        _t_scale = meta['days']
        _type = 'days'
    elif hrs != None : 
        _t_scale = meta['hrs']
        _type = 'hrs'
    elif mins != None : 
        _t_scale = meta['mins']
        _type = 'mins'

    assert type(_t_scale)==int, 'not integer for t_scale'
    query_parameter['start'] = _date
    # delta 시간만큼 시간 변환한 string
    query_parameter['end'] = Utils.strday_delta(_date, _type, _t_scale)
    query_parameter['aggregator'] = meta['aggregator']
    
    metric_list = meta['in_metric'].split('|')
    return_dictbuf=[]
    for _metric in metric_list:
        query_parameter['metric'] = _metric

        query_tags['VEHICLE_NUM'] = meta['carid']
        
        _q_para = query_parameter
        _url = 'http://' + meta['ip'] + ':' + meta['port'] + '/api/query'

        queryData = QueryData(_url, _q_para, query_tags)
        
        _dictbuf, _dictlen = _processingResponse(queryData)
        for _dict in _dictbuf:
            return_dictbuf.append(_dict)
    
    return return_dictbuf, query_parameter['end']

def make_df(_querydata):
    carid=None
    total_dict={}
    for _dict in _querydata:
        carid = _dict['tags']['VEHICLE_NUM']
        if _dict['tags']['SORT'] == 'DRIVE_TIME':
            total_dict[_dict['tags']['SORT']] = sorted(_dict['dps'].items())
        else :
            total_dict[_dict['tags']['SORT']] = sorted(_dict['dps'].items())

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



if __name__ == '__main__':
    
    meta = dict()

    import save_id_list
    
    carid_list = save_id_list.car_id_list

    meta['days'] = 7
    meta['aggregator']='none'
    meta['in_metric'] = 'Elex_calc_length_and_time'
    meta['ip'] = '125.140.110.217'
    meta['port'] = '54242'
    meta['query_start'] = '2019/06/01-00:00:00'
    
    dflist = []
    
    for carid in carid_list:
        meta['carid'] = carid
        queried_data = query_by_timedelta_v3(meta['query_start'], meta, meta['days'])
        if len(queried_data[0]) == 0:
            continue
        new_df = make_df(queried_data[0])
        dflist.append(new_df)
    print('쿼리 및 DataFrame 변환 완료')

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
    print(finaldf)
    print('Excel 파일로 저장중\n')
    writer = pd.ExcelWriter('result.xlsx')
    finaldf.to_excel(writer, sheet_name='Sheet1')
    writer.save()
    writer.close()
    
    length_per_hour_avg = mean[2]
    i=0
    cardict = {}
    for i in range(len(finaldf)-1):
        vehicle_num = finaldf["VEHICLE_NUM"][i]
        if vehicle_num not in cardict.keys():
            cardict[vehicle_num]={}
            cardict[vehicle_num]['DRIVE_LENGTH'] = finaldf["DRIVE_LENGTH"][i]
            cardict[vehicle_num]['DRIVE_TIME'] = finaldf["DRIVE_TIME"][i]
        cardict[vehicle_num]['DRIVE_LENGTH'] += finaldf["DRIVE_LENGTH"][i]
        cardict[vehicle_num]['DRIVE_TIME'] += finaldf["DRIVE_TIME"][i]
    
    print("10대 차량 장/단거리 판단 (시간당 주행 거리 평균 : "+ str(length_per_hour_avg)+")\n")
    for carnum in cardict.keys():
        length_per_hour = float(cardict[carnum]['DRIVE_LENGTH']) * 3600 / cardict[carnum]['DRIVE_TIME'] 
        if length_per_hour >= length_per_hour_avg:
            print('차량 번호 [' + str(carnum) + '] 시간당 이동 거리 ('+ str(format(length_per_hour, '.2f')) + ') : 장거리' )
        else :
            print('차량 번호 [' + str(carnum) + '] 시간당 이동 거리 ('+ str(format(length_per_hour, '.2f')) + ') : 단거리' )