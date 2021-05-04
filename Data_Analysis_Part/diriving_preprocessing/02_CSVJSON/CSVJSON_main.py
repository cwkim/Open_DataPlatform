#!/usr/bin/env python
#-*- coding: utf-8 -*-


from __future__ import print_function
#import requests
import time
import json
from collections import OrderedDict
import pandas as pd
import sys
import os
import argparse
import shutil
import copy
from datetime import datetime

import pcs
#출력 디렉토리 이름을 output으로 변경
# Result, changed JSON 등 , output 디렉토리 하부에 저장
# write 관련 함수는 모듈을 따로 파일로 만들면 좋을것같음

ARG= 50 #argment

def dprint(s): # debug_print
    global g_DEBUG
    if (g_DEBUG):
        print ('  ', s)
    else : return None

def brush_argparse():

    global g_DEBUG # dprint 함수 실행을 위한 플래그

    parser = argparse.ArgumentParser()
    parser.add_argument("-debug", help="debug mode run", action="store_true")
    parser.add_argument("-input_dir", help="select file directory you'll read")
    parser.add_argument("-jsonpack", help="how many json's in one output file", type=int)
    parser.add_argument("-filetype", help="csv or xlsx")
    parser.add_argument("-filekind", help="select file type number you'll store")
    parser.add_argument("-field", help="select field idx you'll store")
    parser.add_argument("-ts", help="select timestamp field idx you'll store")
    parser.add_argument("-carid", help="select carid field idx you'll store")
    parser.add_argument("-metric", help="select metric you'll store")
    parser.add_argument("-outdir", help="select outdir", type=str)
    parser.add_argument("-pn", help="select producer num", default='4')
    parser.add_argument("-cn", help="select consumer num", default='2')
    
    args = parser.parse_args()
    
    if (args.debug) : 
        g_DEBUG = True
        dprint ('DPRINT Enabled ************************************** ' + __file__ )
    
    return args

def make_result_dirctory_tree(_savepath, filepath, _carid, _input_dir, _outdir):
    _savepath = _savepath
    if not(os.path.isdir(_savepath)):
        os.makedirs(os.path.join(_savepath))
    try:
        filepath = filepath.split(_input_dir)[-1]
    except TypeError:
        filepath = filepath.decode('utf-8')
        filepath = filepath.split(_input_dir)[-1]
    if filepath[0] == '/':
        filepath = filepath[1:]
    filepath = filepath.split('/')[0:-1]
    for sub in filepath:
        _savepath = _savepath + '/' + sub
        if not(os.path.isdir(_savepath)):
            os.makedirs(os.path.join(_savepath))
    if _savepath[-1] == '/':
        _savepath = _savepath[0:-1]
    _savepath = _savepath + '/' + _carid
    if not(os.path.isdir(_savepath)):
        os.makedirs(os.path.join(_savepath))
    return _savepath           
                   
# make a file
def writeJson(_buffer, _json_title):
    with open(_json_title+'.json', 'a') as f:
        json.dump(_buffer, f, ensure_ascii=False, indent=4)

# create folder & make files
def writeJsonfiles(_buffer, _json_title, _num, _fname, _carid, _input_dir, _outdir):
    #try:
    #savepath = os.path.dirname(os.path.realpath(__file__))
    #savepath = savepath + _outdir[1:]    
    savepath = _outdir
    '''
    _indx = savepath.find('//')
    if _indx != -1:
        savepath = savepath[:_indx+1] + savepath[_indx+2:] 
    '''
    if not(os.path.isdir(savepath)):
        os.makedirs(os.path.join(savepath))
    #print ('-->',savepath)
    savepath = make_result_dirctory_tree(savepath, _fname, _carid, _input_dir, _outdir)
    # Unicode decode error가 발생하여 수정해줌

    with open(str(savepath + '/' +_json_title+'_'+str(_num)+'.json'), 'w') as f:
        try:
            json.dump(_buffer, f, ensure_ascii=False, indent=4)
            # 몇몇 파일에서 ascii관련 DecodeError 발생
        except:
            json.dump(_buffer, f, ensure_ascii=True, indent=4)
    print ('[' + _json_title + '_'+str(_num)+'.json] saved')
    

# convert Time to Epoch
def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[11:13], _time[14:16], _time[17:])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))
    return epoch

# YYYYmmddHHMMSS -> dd.mm.YY HH:MM:SS
def convertTimeToEpoch_v2(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[6:8], _time[4:6], _time[:4], _time[8:10], _time[10:12], _time[12:])
    pattern = "%d.%m.%Y %H:%M:%S"
    epoch = int (time.mktime(time.strptime(date_time, pattern)))
    return epoch

# display progress bar
def printProgressBar(iteration, total, prefix = u'처리중', suffix = u'완료',\
                      decimals = 1, length = 60, fill = '█'): 
    # 작업의 진행상황을 표시
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' %(prefix, bar, percent, suffix), end='\r')
    sys.stdout.flush()
    if iteration == total:
        None
        #print()

# 파일로 저장하기전 데이터의 정보를 요약해주는 _dataInfo를 만들어줌
def initDataInfo(_dataInfo, __list):
    _dataInfo["1.metric"]= __list[0]["metric"]
    vallist = [d["value"] for d in __list]
    tslist = [int(d["timestamp"]) for d in __list]
    _dataInfo["2.mints"]=min(tslist)
    _dataInfo["3.maxts"]=max(tslist)
    _dataInfo["4.mindt"]=str(datetime.fromtimestamp(_dataInfo["2.mints"]))
    _dataInfo["5.maxdt"]=str(datetime.fromtimestamp(_dataInfo["3.maxts"]))
    _dataInfo["6.totalCnt"]=len(__list)
    _dataInfo["7.minval"]=min(vallist)
    _dataInfo["8.maxval"]=max(vallist)
    _dataInfo["9.tags"]=__list[0]["tags"]
    

def ToJsonFormat(_list, _args_pack_, _json_title, _filename, _input_dir, _outdir):
    
    _list = _list.sort_values(by=[_args_pack_.ts.decode('utf-8')], axis=0)

    Car_id = str(_list[_args_pack_.carid.decode('utf-8')].iloc[0])                
    dftime = _list[_args_pack_.ts.decode('utf-8')].tolist()
    dfval = _list[_args_pack_.field.decode('utf-8')].tolist()
    
    data_len = len(_list)
    _buffer = []
    count=0
    perCount=0
    num=0 #


    for i in range(len(dftime)):
        perCount += 1
        
        value = dfval[i]
        
        # skip NaN value & ts
        if value == 'nan' or dftime[i] == 'nan':
            continue
        elif value == 'NaN' or dftime[i] == 'NaN':
            continue
        
        ts = convertTimeToEpoch(dftime[i])
        ts = str(ts)
        
        csv_data = dict()
        csv_data['metric'] = _args_pack_.metric
        csv_data["tags"] = dict()

        csv_data['timestamp'] = ts
        csv_data["value"] = value
    
        csv_data["tags"]['VEHICLE_NUM'] = str(Car_id)

        if type(_args_pack_.field) == type(b''):
            csv_data["tags"]["fieldname"] = _args_pack_.field.decode('utf-8')
        else:
            csv_data["tags"]["fieldname"] = _args_pack_.field

        count +=  1
        _buffer.append(csv_data)
        
        if count >= _args_pack_.jsonpack:
            dataInfo={}
            initDataInfo(dataInfo, _buffer)
            dataInfo = OrderedDict(sorted(dataInfo.items(), key=lambda t: t[0]))
            _buffer.insert(0, dataInfo)
            num +=1
            writeJsonfiles(_buffer, _json_title, num, _filename, Car_id, _input_dir, _outdir) #save files by bundle
            _buffer = []
            count = 0

        #printProgressBar(perCount, data_len)

    if len(_buffer) != 0:
        # 버퍼에 남은 데이터 json 파일 생성
        #writeJson(_buffer, _json_title)# make a file
        dataInfo={}
        initDataInfo(dataInfo, _buffer)
        dataInfo = OrderedDict(sorted(dataInfo.items(), key=lambda t: t[0]))
        _buffer.insert(0, dataInfo)
        num +=1
        writeJsonfiles(_buffer, _json_title, num, _filename, Car_id, _input_dir, _outdir) #save files by bundle

def field_IndextoStr(_fieldnum, _collist):
    return _collist[_fieldnum]

def ts_IndextoStr(_tsnum, _collist):
    return _collist[_tsnum]

def carid_IndextoStr(_caridnum, _collist):
    return _collist[_caridnum]

def CSVtoDF(_filename, _args_pack_, fieldidx, tsidx, carididx):
    print("\nreading %s" %_filename)
    if _args_pack_.filetype == 'xlsx' :
        df = pd.read_excel(_filename)
    elif _args_pack_.filetype == 'csv' :
        try:
            chunks = pd.read_csv(_filename, usecols = [fieldidx, tsidx, carididx] ,low_memory=False, chunksize=10000, encoding='utf-8')
        except UnicodeDecodeError:
            try:
                chunks = pd.read_csv(_filename,usecols = [fieldidx, tsidx, carididx], low_memory=False, chunksize=10000, encoding='euc-kr')  
            except UnicodeDecodeError:
                chunks = pd.read_csv(_filename, usecols = [fieldidx, tsidx, carididx], low_memory=False, chunksize=10000, encoding='cp949')
        except OSError:
            _filename=_filename.decode('utf-8')
            chunks = pd.read_csv(_filename, usecols = [fieldidx, tsidx, carididx], low_memory=False, chunksize=10000)
    
        df = pd.concat(chunks, ignore_index=True)
    #print(df.columns)

    if _args_pack_.filetype == 'xlsx' :
        try:
            jsonTitle = (_filename.split('/'))[-1][:-5]+'_'+_args_pack_.field
        except TypeError:
            jsonTitle = (_filename.split('/'))[-1][:-5]+'_'+_args_pack_.field.decode('utf-8')
    elif _args_pack_.filetype == 'csv' :
        try:
            jsonTitle = (_filename.split('/'))[-1][:-4]+'_'+_args_pack_.field
        except TypeError:
            jsonTitle = (_filename.split('/'))[-1][:-4]+'_'+_args_pack_.field.decode('utf-8')
    
    print("%s -> DF" %_filename)

    return df, jsonTitle

def pack_to_meta(pack):
    ret = {}
    ret['field']=pack.field
    ret['timestamp']=pack.ts
    ret['carid']=pack.carid
    ret['metric']=pack.metric
    ret['pn']=pack.pn
    ret['cn']=pack.cn
    ret['field']=pack.field
    ret['bundle']=pack.jsonpack
    ret['input_dir']=pack.input_dir
    ret['outdir']=pack.outdir
    return ret

if __name__ == "__main__":
    
    global g_DEBUG
    g_DEBUG = False

    #gFile_type, bundle = brush_args()
    _args_pack_ = brush_argparse()
    _args_pack_.pn = int(_args_pack_.pn)
    _args_pack_.cn = int(_args_pack_.cn)
    dprint (vars(_args_pack_))
    
    input_dir = _args_pack_.input_dir
    outdir = _args_pack_.outdir

    import type_file
    file_type = type_file.file_type
    file_list = file_type['type_'+_args_pack_.filekind]['files']
    col_list = file_type['type_'+_args_pack_.filekind]['columns']
    file_list = [i.encode('utf-8') for i in file_list] #unicode형태(u' ~ ')를 일반 string으로 바꿔줌
    col_list = [i.encode('utf-8') for i in col_list]

    fieldidx = int(_args_pack_.field)
    tsidx = int(_args_pack_.ts)
    carididx = int(_args_pack_.carid)
    # 입력한 index 정보를 실제 string으로 전환
    _args_pack_.field = field_IndextoStr(fieldidx, col_list)
    _args_pack_.ts = ts_IndextoStr(tsidx, col_list)
    _args_pack_.carid = carid_IndextoStr(carididx, col_list)
    #print(_args_pack_.field)
    print('변환 파일 목록')
    for f in file_list:
        print(f)

    np = _args_pack_.pn
    nc = _args_pack_.cn

    meta = pack_to_meta(_args_pack_)
    # 서브 프로세스 관리자, 생산자, 소비자 생성
    workers = pcs.Workers(np, nc)
    works_basket_list = workers.start_work(meta)
    
    for file_name in file_list:
        # csv -> df
        df, title = CSVtoDF(file_name, _args_pack_, fieldidx, tsidx, carididx)
        if len(df) == 0:
            continue
        # df의 크기가 np보다 작으면 main process에서 처리
        if len(df) < np:
            ToJsonFormat(df, _args_pack_, title, file_name, input_dir, outdir)
        # df를 np만큼 분할하여 각 생산자에게 큐로 전송
        else :
            start=0
            end=start+int(len(df)/np)
            for idx in range(np):
                if idx == np-1:
                    end = len(df)
                while (works_basket_list[idx].full()):
                    time.sleep(0.5)
                works_basket_list[idx].put([df[start:end], title, file_name])
                start = end
                end = start+int(len(df)/np)
    print("\nmain : [csv -> df] done")
    print("work basket의 모든 data 전송 완료")
    print("subprocess가 아직 실행 중 입니다...\n")

    lines = workers.report()
    totallines=0
    for line in lines:
        totallines += line
    print("total processed lines : %d" %totallines)
