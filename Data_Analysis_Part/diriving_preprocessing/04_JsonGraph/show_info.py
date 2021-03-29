#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import os
import json

def showInfo():
    import __dataInfo
    datalist = __dataInfo.datalist

    idlist=[]
    fieldlist=[]
    for data in datalist:
        if not data["tags"]["fieldname"] in fieldlist:
            fieldlist.append(data["tags"]["fieldname"])
        if not data["tags"]["VEHICLE_NUM"] in idlist:
            idlist.append(data["tags"]["VEHICLE_NUM"])
    print("\nVEHICLE_NUM")
    idx=1
    for data in idlist:
        print(str(idx) + ". " + data)    
        idx+=1
    print("\nfieldname")
    idx=1
    for data in fieldlist:
        print(str(idx) + ". " + data)   
        idx+=1 
    print('')
    

            

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

if __name__ == "__main__":
    renewal = sys.argv[1]
    if renewal == 'o':
        showInfo()
        exit()
    json_path = sys.argv[2]
    if json_path[-1] == '/':
        json_path = json_path[0:-1]
    print("검색 대상 디렉토리 경로 : " + json_path)
    
    file_list = []
    recursive_search_dir(json_path, file_list)

    _buffer = []
    count = 0
    loop_count = 0

    tot_file_len = len(file_list)
    print('총 파일 개수 : %s\n' %tot_file_len)
    
    datalist=[]
    idx = 0
    while idx < len(file_list):
        file_path = file_list[idx]
        print(file_path)
        with open(file_path,'r') as f:
            _list = json.loads(f.read())
        if len(_list)==0:
            idx+=1
            continue
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
                    if len(t_list)==0:
                        idx+=1
                        continue
                    if len(t_list[0]) > 4:
                        t_list = t_list[1:-1]
                _list = _list+t_list
            else:
                idx-=1
                break
        if len(_list[0]) > 4:
            _list = _list[1:-1]

        if len(_list)==0:
            idx+=1
            continue
        
        data={}
        
        tslist = [int(d["timestamp"]) for d in _list]
        vallist = [d["value"] for d in _list]
        data["tslist"] = tslist
        data["vallist"] = vallist
        
        tags = _list[0]["tags"]
        data["tags"] = tags
        datalist.append(data)

        idx+=1

    with open('./__dataInfo.py', 'w') as f:
        f.write('datalist=%s\n' %datalist)
    print('')
    showInfo()
