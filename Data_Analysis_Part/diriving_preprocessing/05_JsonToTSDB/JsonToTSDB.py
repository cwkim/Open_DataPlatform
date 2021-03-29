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
import curses


# 상태 프로그레스 출력 함수
# 2019/10/09 1줄 출력에 다양한 정보 추가
# to do : 여러줄로 표시하는 기능 추가 필요 
def printProgressBar(iteration, total, sub_count=0,_lastmsg='', prefix = '',\
                     suffix = 'Done',\
                     decimals = 1, length = 25, fill = '█'): 

    global _sub_indicator_ 

    # 작업의 진행상황을 표시
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    
    # 2단계 세부 진행상황을 표시하기 위한 내용
    _out_msg_ = '  ' + _sub_indicator_[sub_count]
    #print ( ' ', [count%4], end='\r' )
    
    # 앞의 세칸은 진행중임을 알려주기 위한 애니메이션 표시용 임
    _out_msg_ += ' : %s |%s| %s%% %s' %(prefix, bar, percent, suffix) 
    _out_msg_ += '   ' + _lastmsg + '\r' 

    sys.stdout.write(_out_msg_)
    sys.stdout.flush()
    
    #if iteration == total:
    #    print()


######################   OpenTSDB   #############################
def put_data(_list, __opentsdb_url, sess):
    headers = { 'content-type' : 'application/json' }
    # 세션의 경우는 proc 이 실행되는 동안 
    # 1회만 활성화 되면 충분한것 같아서, main 에서 호출하도록 위치 수정
    #sess = requests.Session()

    url = __opentsdb_url+str('/api/put')
    
    try:
        response = sess.post(url, data=json.dumps(_list), headers = headers)

        while response.status_code > 204:
            print("[Bad Request] Put status: %s" % (response.status_code))
            print("[Bad Request] we got bad request, Put will be restarted after 3 sec!\n")
            time.sleep(3)
            
            print("[Put]" + json.dumps(_list, ensure_ascii=False, indent=4))
            response = sess.post(url, data=json.dumps(_list), headers = headers)

        #print ("[Put finish and out]")

    except Exception as e:
        print("[exception] : %s" % (e))


def dprint(s): # debug_print
    global g_DEBUG
    if (g_DEBUG):
        print ('  ', s)
    else : return None

def brush_argparse():

    global g_DEBUG # dprint 함수 실행을 위한 플래그
    global g_dbtype # 글로벌 ['ots', 'influx', 'redis', 'couch']

    parser = argparse.ArgumentParser()
    parser.add_argument("-debug", help="debug mode run", action="store_true")
    parser.add_argument("-dbtype", help="타겟(output) DB 종류 select (ots, influx, redis, couch), openTSDB, influxDB, RedisDB, Couchbase")
    parser.add_argument("-input", help="입력(input)파일 JSON 위치 디렉토리 이름") #, type=int)
    parser.add_argument("-url", help="입력 DB url") #, type=str)

    args = parser.parse_args()
    
    if (args.debug) : 
        g_DEBUG = True
        dprint ('DPRINT Enabled ************************************** ' + __file__ )
    
    return args

def listup_args(_i):
    # 실행했을때 결과를 아래 줄에 메모하여 코드 진행을 이해하도록 함
    # 실행시 Namespace 참고. (주의) main 함수에서의 처리에 따라 달라질수 있음 
    # Namespace(dbtype=None, debug=False, input='./weekData')
    #print (_i)
    #exit()

    global g_dbtype # 글로벌 ['ots', 'influx', 'redis', 'couch']

    
    if (_i.dbtype not in g_dbtype) or (_i.dbtype == None) :
        print ('   [***FORCE by CODE***] now using open tsdb, otherwise, Please input proper db type')
        _i.dbtype = 'ots'

    if _i.url == None:
        _i.url = 'def'
        
    if _i.input == None:
        print (" [Input required], input json file name or dir name")
        print (" Please check -h option, python {filename}.py -h")
        exit(" close running process")

    return _i.dbtype, _i.debug, _i.input 

def recursive_search_dir(_nowDir, _filelist):
    f_list = os.listdir(_nowDir)
    dir_list=[]
    total_size = 0
    for fname in f_list:
        if os.path.isdir(_nowDir+'/'+fname):
            dir_list.append(_nowDir+'/'+fname)
        elif os.path.isfile(_nowDir+'/'+fname):
            _filelist.append(_nowDir+'/'+fname)
            total_size += os.path.getsize(_nowDir+'/'+fname)
    
    for toDir in dir_list:
        dprint ( '... directory processing ... ' + toDir)
        total_size += recursive_search_dir(toDir, _filelist)
    
    return total_size


if __name__ =='__main__':
    global g_DEBUG
    global _sub_indicator_ 
    _sub_indicator_ = ['-', '\\', '-', '/']
    _sub_ind_len_ = len(_sub_indicator_)
    global g_dbtype
    g_dbtype = ['ots', 'influx', 'redis', 'couch']

    
    sess = requests.Session()

    _args_ = brush_argparse()
    #dprint(_args_) # 확인 필요시에 출력하여 내용 확인
    dbtype, g_DEBUG, inputdir = listup_args(_args_)
    opentsdb_url = _args_.url
    root_path = inputdir
    
    #기존 입력 디렉토리 경로, 위의 inputdir 이 동작을 잘 하면, 삭제 예정
    file_list = []
    recursive_search_dir(root_path, file_list)

    _buffer = []
    count = 0
    loop_count = 0

    tot_file_len = len(file_list)
    print('총 파일 개수 : %s' %tot_file_len)
    _last_msg = ''

    for file_path in file_list:
        _last_msg = file_path
        with open(file_path,'r') as f:
            _list = json.loads(f.read())
            #print ('갯수 : ',len(_list))
            if len(_list[0]) > 4:
                _list = _list[1:]
            _last_msg = file_path + ' JOSN size : ' + str(len(_list))

        # print (' ... starting one of JSON files ')       
        for data_dict in _list:
            # 50개 단위 JSON 처리 
            data_len = len(_list)
            _buffer.append(data_dict)
            count += 1
            
            # itteration 표현방법, 일반 라인 표시 방법 과 문장 앞 애니메이션
            #print(' '* 65, '   itteration count %s' %str(count), end='\r')
            #print ( ' ', ['-', '\\', '-', '/'][count%4], end='\r' )

            sub_count = count % _sub_ind_len_
            

            if count >= 50:
                put_data(_buffer, opentsdb_url, sess)
                _buffer = []
                count = 0
        # 50개 단위로 처리하고, 마지막 남은 JSON 전송
        if len(_buffer) != 0:
            put_data(_buffer, opentsdb_url, sess)
            _buffer = []
            count = 0
        loop_count += 1
        printProgressBar(loop_count, tot_file_len, sub_count, _last_msg)
        
        # sess close 필요여부 확인 하여 코드 추가
