# -*- coding: utf-8 -*-
# Author : ChulseoungChae , https://github.com/ChulseoungChae

from __future__ import print_function
import os
import time
import datetime
import sys
import json
import requests
from collections import OrderedDict
from bs4 import BeautifulSoup
import urllib
import importlib
import argparse
import pprint

query_parameter = {
    "start" : "2014-06-01 00:00:00",
    "end": "2014-06-02 00:00:00",
    "aggregator" : "none",
    "metric" : "____test____"
}

""" 쿼리 하려는 tag """
query_tags = {
}


def brush_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m_num", help="metric number")
    parser.add_argument("-ip", help="openTSDB ip")
    parser.add_argument("-port", help="openTSDB port")
    parser.add_argument("-metric", help="openTSDB metric")
    parser.add_argument("-start", help="start date for query")
    parser.add_argument("-end", help="end date for query")
    parser.add_argument("-field", help="field for query")
    parser.add_argument("-img_filename", help="Folder where the image will be saved")
        
    args = parser.parse_args()
    
    return args


def calc(buf, meta) :
    car_id_list = []
    for data in buf:
        carid = data['tags'][meta['tags1']]
        car_id_list.append(carid)

    return car_id_list


def get_ids(meta):
    id_list = []
    mem_old = -1
    
    date_from = meta['start']
    date_to = meta['end']
    dys = meta['days']
    hrs = meta['hrs']
    mins = meta['mins']

    while(date_from != None):
        queried_data, date_end = query_by_timedelta(date_from, meta, dys=dys, hrs=hrs, mins=mins)
      
        if date_to <= date_end : date_end = None
        
        if len(queried_data) != 0:
            returned_id_list = calc(queried_data, meta)
            id_list += returned_id_list
            id_list = list(set(id_list))
            mem_count = ( len(id_list) )

            if (mem_old != mem_count) : 
                pout = ' search IDs ' + str(mem_count)+'\r'
                sys.stdout.write(pout)
                sys.stdout.flush()

            mem_old = mem_count

        date_from = date_end

    return_data = id_list

    return return_data


def query_by_timedelta(_date, meta, dys=None, hrs=None, mins=None):
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
    query_parameter['end'] = strday_delta(_date, _type, _t_scale)
    query_parameter['aggregator'] = 'none'
    query_parameter['metric'] = meta['metric']

    query_tags[meta['tags1']] = '*'
    query_tags[meta['tags2']] = meta['field']

    _q_para = query_parameter
    _url = 'http://' + meta['ip'] + ':' + meta['port'] + '/api/query'

    queryData = QueryData(_url, _q_para, query_tags)
    _dictbuf, _dictlen = _processingResponse(queryData)
    
    return _dictbuf, query_parameter['end']


def QueryData(_url, _required, _tags=None):
    headers = {'content-type': 'application/json'}

    dp = OrderedDict()
    dp["start"] = convertTimeToEpoch(_required["start"])
    dp["end"] = convertTimeToEpoch(_required["end"]) - int(1)

    temp = OrderedDict()
    temp["aggregator"] = _required["aggregator"]
    temp["metric"] = _required["metric"]
    if _tags != None:
        temp["tags"] = _tags

    dp["queries"] = []
    dp["queries"].append(temp)

    response = requests.post(_url, data=json.dumps(dp), headers= headers)

    while response.status_code > 204:
        print(" [Bad Request] Query status: %s" % (response.status_code))
        print(" [Bad Request] We got bad request, Query will be restarted after 3 sec!\n")
        time.sleep(3)

        print(" [Querying]" + json.dumps(dp, ensure_ascii=False, indent=4))
        response = requests.post(_url, data=json.dumps(dp), headers= headers)

    return response


### 차량 id list 저장
def savefile(buf):
    f = open('id_list.py', 'w')
    f.write('id_list=%s\n' % buf)
    f.close()

### ========== 형변환 함수들 ==========
def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[-8:-6], _time[-5:-3], _time[-2:])
    pattern = '%d.%m.%Y %H:%M:%S'
    epoch = int (time.mktime(time.strptime(date_time, pattern)))
    return epoch


def _processingResponse(in_data):
    '''openTSDB에서 전송받은, string 들을 dictionary로 변경'''
    _d = in_data
    _r = _d.json()
    _l = len(_r)

    return _r, _l


def strday_delta(_s, _type, _h_scale):
    _dt  = str2datetime(_s)
    if _type == 'days' :
        _dt += datetime.timedelta(days = _h_scale)
    elif _type == 'hrs':
        _dt += datetime.timedelta(hours = _h_scale)
    elif _type == 'mins':
        _dt += datetime.timedelta(minutes = _h_scale)

    _str = datetime2str(_dt)
    return _str

def datetime2str(dt):
    return dt.strftime('%Y/%m/%d-%H:%M:%S')


def str2datetime(dt_str):
    return datetime.datetime.strptime(dt_str, "%Y/%m/%d-%H:%M:%S")
### ===============================


def var_save(_var, file_name):
    f = open(file_name, 'w')
    f.write('meta=%s\n' % _var)
    f.close()


if __name__ == '__main__':
    _args_ = brush_argparse()

    var_file_name = 'var.py'
    meta = dict()

    if os.path.isfile(var_file_name) == True:
        import_file = importlib.import_module(var_file_name[:-3])
        meta = import_file.meta

    metric_num = _args_.m_num

    if _args_.ip != 'd':
        meta['ip'] = _args_.ip
    if _args_.port != 'd':
        meta['port'] = _args_.port
    if _args_.start != 'd':
        meta['start'] = _args_.start
    if _args_.end != 'd':
        meta['end'] = _args_.end

    if metric_num == '1':
        if _args_.field != 'd':
            meta['field'] = _args_.field
    elif metric_num == '2':
        if _args_.field != 'd':
            field_list = _args_.field.split('/')
            meta['field'] = field_list[0]
            meta['field2'] = field_list[1]

    if metric_num == '1':
        if _args_.metric != 'd':
            meta['metric'] = _args_.metric
    elif metric_num == '2':
        if _args_.metric != 'd':
            metric_list = _args_.metric.split('/')
            meta['metric'] = metric_list[0]
            meta['metric2'] = metric_list[1]

    if _args_.img_filename == 'd':
        img_filename = './img/'
    else:
        img_filename = _args_.img_filename

    meta['days'] = 1
    meta['hrs'] = None
    meta['mins'] = None
    meta['tags1'] = 'VEHICLE_NUM'
    meta['tags2'] = 'fieldname'

    pprint.pprint(meta)

    var_save(meta, var_file_name)

    if not os.path.isdir(img_filename):
        os.mkdir(img_filename)

    if os.path.isfile('id_list.py') == False :
        result = get_ids(meta)
        savefile(result)
        carid_list = result
    else:
        import_file = importlib.import_module('id_list')
        carid_list = import_file.id_list
        #print(carid_list)

    ### TSDB 그래프 URL 변수에 저장
    if metric_num == '1':
        url1 = 'http://'+meta['ip']+':'+meta['port']+'/q?start='+meta['start']+'&end='+meta['end']
        url2 = '&m=none:'+meta['metric']+'%7B'+meta['tags1']+'='
        url3 = ','+meta['tags2']+'='+meta['field']+'%7D&o=&yrange=%5B0:%5D&wxh=1516x600&style=linespoint&png'

        sub_dir_name = img_filename+'unit_metric/'
        if not os.path.isdir(sub_dir_name):
                os.mkdir(sub_dir_name)

        ### 차량 id별로 사용자가 입력한 기간, field에 대한 그래프 이미지파일로 저장
        _count = 0
        for i in carid_list:
            _count += 1
            fin_url = url1+url2+str(i)+url3
            _img = urllib.urlopen(fin_url).read()
            
            with open(sub_dir_name+str(i)+'.png', mode='wb') as f:
                f.write(_img)
                print('%s / %s, id = %s finish' %(_count, len(carid_list), i))

    elif metric_num == '2':
        url1 = 'http://'+meta['ip']+':'+meta['port']+'/q?start='+meta['start']+'&end='+meta['end']
        url2 = '&m=none:'+meta['metric']+'%7B'+meta['tags1']+'='
        url3 = ','+meta['tags2']+'='+meta['field']+'%7D&o=&m=none:'+meta['metric2']+'%7B'+meta['tags1']+'='
        url4 = ','+meta['tags2']+'='+meta['field2']+'%7D&o=&yrange=%5B0:%5D&wxh=1516x600&style=linespoint&png'

        sub_dir_name = img_filename+'two_metric/'
        if not os.path.isdir(sub_dir_name):
                os.mkdir(sub_dir_name)

        _count = 0
        for i in carid_list:
            _count += 1
            fin_url = url1+url2+str(i)+url3+str(i)+url4
            _img = urllib.urlopen(fin_url).read()
            
            with open(sub_dir_name+str(i)+'.png', mode='wb') as f:
                f.write(_img)
                print('%s / %s, id = %s finish' %(_count, len(carid_list), i))