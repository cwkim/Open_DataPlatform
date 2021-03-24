#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import requests
import time
import json
from collections import OrderedDict
import pandas as pd
import sys
import os
import argparse
import shutil
import copy
import datetime
import requests



# YYYY-mm-dd HH:MM:SS -> epoch
def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" % (_time[8:10], _time[5:7], _time[:4], _time[11:13], _time[14:16], _time[17:19])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int(time.mktime(time.strptime(date_time, pattern)))
    return epoch


def _processingResponse(in_data):
    '''openTSDB에서 전송받은, string 들을 dictionary로 변경하여
       dict 를 회신함. 형태를 변경하고, dict의 갯수를 알려줌'''
    _d = in_data
    _r = _d.json()
    # queryData.fieldname is string, thus convert this to list
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

    #print " [Querying]" + json.dumps(dp, ensure_ascii=False, indent=4)
    response = requests.post(_url, data=json.dumps(dp), headers= headers)

    while response.status_code > 204:
        print(" [Bad Request] Query status: %s" % (response.status_code))
        print(" [Bad Request] We got bad request, Query will be restarted after 3 sec!\n")
        time.sleep(3)

        print(" [Querying]" + json.dumps(dp, ensure_ascii=False, indent=4))
        response = requests.post(_url, data=json.dumps(dp), headers= headers)

    pout = " [Query is done, got reponse from server]" + __file__
    pout += " : now starting processing, writing and more "
    return response


def str2datetime(dt_str):
    return datetime.datetime.strptime(dt_str, "%Y/%m/%d-%H:%M:%S")


def datetime2str(dt):
    return dt.strftime('%Y/%m/%d-%H:%M:%S')


def strday_delta(_s, _type, _h_scale):
    _dt  = str2datetime(_s)
    if _type == 'days' :
        _dt += datetime.timedelta(days = _h_scale)
    elif _type == 'hrs':
        _dt += datetime.timedelta(hours = _h_scale)
    elif _type == 'mins':
        _dt += datetime.timedelta(minutes = _h_scale)
    #if _h_scale == 24 : _dt += datetime.timedelta(days = 1)
    #elif _h_scale == 1 : _dt += datetime.timedelta(hours = 1)
    #elif _h_scale == 0.1 : _dt += datetime.timedelta(minutes = 20)
    _str = datetime2str(_dt)
    return _str


# 여러 메트릭에서 쿼리
def query_by_timedelta_v3(_date, meta, dys=None, hrs=None, mins=None):
    query_parameter = {}
    query_tags = {}
    
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
    query_parameter['end'] = strday_delta(_date, _type, _t_scale)
    query_parameter['aggregator'] = 'none'
    
    metric_list = meta['metric'].split('|')
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


def query_nto_tsdb(meta):
    st = time.time()

    date_from = meta['query_start']
    date_to = meta['query_end']
    dys = meta['days']
    hrs = meta['hrs']
    mins = meta['mins']

    if type(meta['carid']) == list:
        carid_list = meta['carid']
    else:
        carid_list = meta['carid'].split('|')
    
    ret_dict={}
    # process start
    while(date_from != None):

        for carid in carid_list:
            
            meta['carid'] = carid
            start_time = time.time()
            queried_data, date_end = query_by_timedelta_v3(date_from, meta, dys=dys, hrs=hrs, mins=mins)
            
            if date_to <= date_end : date_end = None

            for _dict in queried_data:
                while True:
                    try:
                        ret_dict[_dict['tags']['VEHICLE_NUM']][_dict['tags']['fieldname']]+=len(_dict['dps'])
                        break
                    except KeyError:
                        try:
                            ret_dict[_dict['tags']['VEHICLE_NUM']][_dict['tags']['fieldname']]=0
                        except KeyError:
                            ret_dict[_dict['tags']['VEHICLE_NUM']]={}

                
            run_time = time.time() - start_time
            print(" Run time for query: %.4f (sec) | [carid] : [%s]" % (run_time, carid))
        
        date_from = date_end 
    et = time.time()
    print('\n    '+
        ' Query Start : ' +meta['query_start'] + '\n    ' + 
        ' Query End : ' + meta['query_end'] + '\n    ' +
        ' Time unit : ' + meta['timeunit'] + '\n    ' +
        ' Time long : ' + meta['timelong'] + '\n    ' +
        ' Query url:port : ' + meta['ip']+':'+meta['port'] +'\n    '+
        ' Query Metric : ' + meta['metric'] + '\n    '+ 
        ' Carid : ' + meta['carid'] + '\n    ' +
        ' Total Time : ' + str(et-st))
    total_dps = 0
    for carid in ret_dict:
        print('\nCARID : ' + carid)
        for fieldname in ret_dict[carid]:
            print('  FIELDNAME : ' + fieldname + ' = ' + str(ret_dict[carid][fieldname]) + ' dps')
            total_dps += ret_dict[carid][fieldname]
    print('\nTOTAL data points = ' + str(total_dps) + '\n')

    with open('./out.txt', 'w') as f:
        f.write('\n    '+
        ' Query Start : ' +meta['query_start'] + '\n    ' + 
        ' Query End : ' + meta['query_end'] + '\n    ' +
        ' Time unit : ' + meta['timeunit'] + '\n    ' +
        ' Time long : ' + meta['timelong'] + '\n    ' +
        ' Query url:port : ' + meta['ip']+':'+meta['port'] +'\n    '+
        ' Query Metric : ' + meta['metric'] + '\n    '+ 
        ' Carid : ' + meta['carid'] + '\n    ' +
        ' Total Time : ' + str(et-st))
        total_dps = 0
        for carid in ret_dict:
            f.write('\n\nCARID : ' + carid)
            for fieldname in ret_dict[carid]:
                f.write('\n  FIELDNAME : ' + fieldname + ' = ' + str(ret_dict[carid][fieldname]) + ' dps')
                total_dps += ret_dict[carid][fieldname]
        f.write('\n\nTOTAL data points = ' + str(total_dps) + '\n')


def brush_args():
    
    _len = len(sys.argv)
    #print("sys len : %d" %(_len))
    if _len < 9:
        print(" 추가 정보를 입력해 주세요. 위 usage 설명을 참고해 주십시오")
        print(" python 실행파일을 포함하여 아규먼트 갯수 10개 필요 ")
        print(" 현재 아규먼트 %s 개가 입력되었습니다." % (_len))
        print(" check *run.sh* file ")
        exit(" You need to input more arguments, please try again \n")
    _ip = sys.argv[1]
    _port = sys.argv[2]
    _metric = sys.argv[3]
    _start = sys.argv[4]
    _end = sys.argv[5]
    _carid = sys.argv[6]
    _timeunit = sys.argv[7]
    _timelong = sys.argv[8]

    #_sort 제외
    return _ip, _port, _metric, _start, _end, _carid, _timeunit, _timelong


if __name__ == "__main__":
    ip, port, in_metric, q_start, q_end, carid, timeunit, timelong = brush_args()

    meta = dict()  # 메타정보 저장할 딕셔너리 생성

    meta['ip'] = ip  # read 아이피주소
    meta['port'] = port  # read포트번호
    meta['metric'] = in_metric  # read메트릭이름

    meta['query_start'] = q_start  # 시작시간
    meta['query_end'] = q_end  # 종료시간

    if carid == 'none':
        meta['carid'] = '*'
    
    meta['timeunit'] = timeunit
    meta['timelong'] = timelong

    # 여기서 시간 loop 에 사용할 변수 정리
    # 처리할 시간단위, 정리
    meta['days'] = None
    meta['hrs'] = None
    meta['mins'] = None

    _tlong = meta['timelong']

    _tlong = int(_tlong, base=10)
    if meta['timeunit'] == 'd':
        meta['days'] = _tlong

    elif meta['timeunit'] == 'h':
        meta['hrs'] = _tlong

    elif meta['timeunit'] == 'm':
        meta['mins'] = _tlong

    st = time.time()
    query_nto_tsdb(meta)
    et = time.time()
    print('Time elapsed : ',et-st)
