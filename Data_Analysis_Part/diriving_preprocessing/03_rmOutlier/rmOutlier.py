#-*- coding:utf-8 -*-

import time
import os
import sys
import json
from datetime import datetime
import collections
import argparse

# 속도가 0보다 작거나 260보다 크면 이상치로 결정
def rmOutlier_DRIVE_SPEED(_list, idx ,outlier):
    Val = _list[idx]['value']
    if Val < 0 or Val > 260:
        _list[idx]['tags']['fieldname'] = 'DRIVE_SPEED_1_OUT'
        outlier.append(_list[idx])
        del _list[idx]
        return -1
    return 0
    
# 주행 거리가 급격하게 증가하거나 감소하는 데이터를 이상치로 결정
def rmOutlier_DRIVE_LENGTH_TOTAL(_list, idx ,outlier):
    # cons = 주행거리 단위를 맞추기 위한 변수 (cons = 1->km, 1000->m)
    cons=1
    if _list[idx]['metric'] == 'hanuri_origin':
        cons=1000.0

    nextidx=idx+1
    if nextidx >= len(_list):
        _list[idx]['value']/=cons
        return 0
    
    # 올바른 첫 데이터 탐색
    while True:
        if len(_list) == 0:
            break
        if _list[idx]['value'] <= 0:
            _list[idx]['tags']['fieldname'] = 'DRIVE_LENGTH_TOTAL_1_OUT'
            outlier.append(_list[idx])
            del _list[idx]
        else:
            break
    
    # 동일한 데이터일 시 제거
    while True:
        if nextidx >= len(_list):
            return 0
        tsdif = int(_list[nextidx]['timestamp']) - int(_list[idx]['timestamp'])
        if tsdif == 0:
            del _list[nextidx]
        else:
            break

    _list[idx]['value']/=cons
    preval = _list[idx]['value']

    # 누적주행거리가 비정상정으로 증가하거나 감소할시 이상치로 판단
    # 1초당 이동할 수 있는 최대 거리 0.073km (최대시속 260km 기준)
    # 최소 14초가 지나야 1km이상 이동 가능
    # 하지만 실제 데이터 값은 정수형이므로 13초 사이에 2km이상(1km초과) 이동하면 이상치로 판단
    while True:
        if nextidx >= len(_list):
            return 0
        tsdif = int(_list[nextidx]['timestamp'])- int(_list[idx]['timestamp'])
        nextval = _list[nextidx]['value']/cons
        if nextval - preval > 1 * int(tsdif/14) + 2 or nextval - preval < 0:
            _list[nextidx]['tags']['fieldname'] = 'DRIVE_LENGTH_TOTAL_1_OUT'
            outlier.append(_list[nextidx])
            del _list[nextidx]
        else:
            break
    return 0

# 0 <= RPM <= 8000 이외의 데이터를 이상치로 결정 - 일반적으로 승용차 8~9000 RPM이 RED Zone 이었음
def rmOutlier_RPM(_list, idx ,outlier):
    Val = _list[idx]['value']
    if Val < 0 or Val > 8000:
        _list[idx]['tags']['fieldname'] = 'RPM_1_OUT'
        outlier.append(_list[idx])
        del _list[idx]
        return -1
    return 0

def makesubDir(_fname, _input_dir, _output_dir, dir_name):
    try:
        #directory = os.path.dirname(os.path.realpath(__file__))
        directory = _output_dir
        path = directory + '/' + dir_name
        if not(os.path.isdir(path)):
            os.makedirs(os.path.join(path))
        dir_list = _fname.split(_input_dir+'/')[-1]
        dir_list = dir_list.split('/')[0:-1]
        for dirname in dir_list:
            path = path + '/' + dirname
            if not(os.path.isdir(path)):
                os.makedirs(os.path.join(path))
        
    except OSError as e:
        if e.errno != errno.EEXIST:
            print("Failed to create directory!!!")
            raise

    return path

# create folder & make files
def writeJsonfiles(_buffer, _json_title, _num, _path):
    # Unicode decode error가 발생하여 수정해줌
    try:
        with open(str(unicode(_path + '/' +_json_title+str(_num)+'.json')), 'w') as f:
            try:
                json.dump(_buffer, f, ensure_ascii=False, indent=4)
                # 몇몇 파일에서 ascii관련 DecodeError 발생
            except:
                json.dump(_buffer, f, ensure_ascii=True, indent=4)
    except NameError:
        with open(_path + '/' +_json_title+str(_num)+'.json', 'w') as f:
            try:
                json.dump(_buffer, f, ensure_ascii=False, indent=4)
                # 몇몇 파일에서 ascii관련 DecodeError 발생
            except:
                json.dump(_buffer, f, ensure_ascii=True, indent=4)

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
        total_size += recursive_search_dir(toDir, _filelist)
    
    return total_size

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

def writeByBundle(_list, _title, _sub_path, bundle):
    start=0
    end=start+bundle
    num=0
    while True:
        # 각 파일의 정보를 저장하는 변수
        if end >= len(_list):
            break
        num+=1
        dataInfo={}
        initDataInfo(dataInfo, _list[start:end])
        dataInfo = collections.OrderedDict(sorted(dataInfo.items(), key=lambda t: t[0]))

        _buffer = _list[start:end]
        _buffer.insert(0, dataInfo)
        writeJsonfiles(_buffer, _title, num, _sub_path)
        start = end
        end +=bundle
    if end != len(_list):
        num+=1
        dataInfo={}
        initDataInfo(dataInfo, _list[start:len(_list)])
        dataInfo = collections.OrderedDict(sorted(dataInfo.items(), key=lambda t: t[0]))
        _buffer = _list[start:len(_list)]
        _buffer.insert(0, dataInfo)
        writeJsonfiles(_buffer, _title, num, _sub_path)

def brush_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-input_dir", help="json파일이 저장된 디렉토리")
    parser.add_argument("-output_dir", help="결과파일이 저장될 디렉토리")
    parser.add_argument("-bundle", help="json파일을 저장시 묶을 json data 수")

    args = parser.parse_args()
    
    return args

if __name__ =='__main__':
    
    # 아규먼트 처리 
    _args_ = brush_argparse()
    input_dir = _args_.input_dir
    output_dir = _args_.output_dir
    '''
    root_path = os.path.dirname(os.path.realpath(__file__))
    if root_path[-1] != '/':
        root_path = root_path+'/'
    input_dir = root_path+input_dir
    '''
    bundle = int(_args_.bundle)
    
    print("검색 대상 디렉토리 경로 : " + input_dir)
    print("파일당 저장 데이터 개수 : " + str(bundle))
    file_list = []
    recursive_search_dir(input_dir, file_list)

    _buffer = []
    count = 0
    loop_count = 0

    tot_file_len = len(file_list)
    print('총 파일 개수 : %s\n' %tot_file_len)
    idx = 0
    while idx < len(file_list):
        file_path = file_list[idx]
        print(file_path)
        with open(file_path,'r') as f:
            _list = json.loads(f.read())
        
        # 파일로 분리된 필드 data를 합쳐줌
        while True:
            idx+=1
            if idx >= len(file_list):
                idx-=1
                break
            tmp1 = (file_path.split('/')[-1]).split('_')[0:-1]
            tmp2 = (file_list[idx].split('/')[-1]).split('_')[0:-1]
            if tmp1 == tmp2:
                print(file_list[idx])
                with open(file_list[idx], 'r') as f:
                    t_list = json.loads(f.read())
                if len(t_list[0]) > 4 :
                    t_list = t_list[1:-1]
                _list = _list+t_list
            else:
                idx-=1
                break
        if len(_list[0]) > 4 :
            _list = _list[1:-1]
        if len(_list)==0:
            print("데이터 개수 0")
            idx+=1
            continue
        print("파일로 분리된 데이터 결집 완료\n")
        print("이상치 검색 시작")
        # timestamp 순으로 정렬
        _list = sorted(_list, key=lambda k: k['timestamp'])
        Outlierlist = [] # 이상치값 저장될 list
        idx2=0
        while idx2 < len(_list):
            ret=0
            if _list[idx2]['tags']['fieldname'] == 'DRIVE_SPEED':
                #_list[idx2]['tags']['fieldname'] = 'DRIVE_SPEED_1'
                ret = rmOutlier_DRIVE_SPEED(_list, idx2, Outlierlist)
            elif _list[idx2]['tags']['fieldname'] == 'DRIVE_LENGTH_TOTAL':
                #_list[idx2]['tags']['fieldname'] = 'DRIVE_LENGTH_TOTAL_1'
                ret = rmOutlier_DRIVE_LENGTH_TOTAL(_list, idx2, Outlierlist)
            elif _list[idx2]['tags']['fieldname'] == 'RPM':
                #_list[idx2]['tags']['fieldname'] = 'RPM_1'
                ret = rmOutlier_RPM(_list, idx2, Outlierlist)
            else:
                None
            idx2+=ret
            idx2+=1
        print("이상치 제거 완료\n")
        if len(_list)==0:
            print("데이터 개수 0")
            idx+=1
            continue

        # 이상치 제거한 리스트 파일로 저장
        print("이상치 제거한 데이터 파일로 저장 시작")
        _title = ""
        tmp = (file_path.split('/')[-1]).split('_')[0:-1]
        for part in tmp:
            _title = _title + part + '_'
        sub_path = makesubDir(file_path, input_dir, output_dir, "cleanupData")
        writeByBundle(_list, _title, sub_path, bundle)
        print("저장 완료\n")

        # 이상치 데이터 파일로 저장
        if len(Outlierlist) != 0:
            print("이상치 데이터 파일로 저장 시작")
            _title = _title + 'OUT_'
            sub_path = makesubDir(file_path, input_dir, output_dir, "outlierData")
            writeByBundle(Outlierlist, _title, sub_path, bundle)
            print("저장 완료\n")

        idx+=1
