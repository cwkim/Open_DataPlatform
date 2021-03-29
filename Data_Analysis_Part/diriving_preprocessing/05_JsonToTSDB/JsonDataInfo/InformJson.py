#!/usr/bin/env python
#-*- coding: utf-8 -*-

import os
import sys
import json
import time 

# json 파일을 찾기위해 서브 디렉토리 탐색
def recursive_search_dir(_nowDir, _filelist):
    dir_list = []
    f_list = os.listdir(_nowDir)
    for fname in f_list:
        if os.path.isdir(_nowDir+'/'+fname):
            dir_list.append(_nowDir+'/'+fname)
        elif os.path.isfile(_nowDir+'/'+fname):
            if fname[-4:] == 'json' or fname[-4:] == 'JSON':
                _filelist.append(_nowDir+'/'+fname)

    for toDir in dir_list:
        recursive_search_dir(toDir, _filelist)

# 파일 리스트 중 새로 갱신될 파일을 걸러냄
def cleanup_filelist(_filelist):
    # 삭제된 파일 갱신
    import __JsonDataInfo
    json_info = __JsonDataInfo.JsonDataInfo
    print("\n삭제된 파일")
    del_dict={}
    for filename in json_info.keys():
        for filepath in json_info[filename]["files"]:
            if not os.path.isfile(filepath):
                del_dict[filename] = True
                print(filepath)
    
    for filename in del_dict.keys():
        del json_info[filename]
    # 새로 추가될 파일 갱신
    keydict = {}
    newfilelist = []
    for file_path in _filelist:
        file_name = file_path.split('/')[-1]
        file_name_part = file_name.split('_')
        file_name_part = file_name_part[0:-1]
        file_name=""
        for name_part in file_name_part:
            file_name = file_name + name_part + '_'
        file_name = file_name[0:-1]
        if not file_name in json_info.keys():
            newfilelist.append(file_path)
        else :
            if file_path not in json_info[file_name]["files"]:
                newfilelist.append(file_path)
 
    print('')
    with open(root_path+'/__JsonDataInfo.py', 'w') as f:
        f.write('JsonDataInfo=%s\n' %json_info)
    return newfilelist


if __name__ == '__main__':
    root_path = os.path.dirname(os.path.realpath(__file__))

    if len(sys.argv) < 3 :
        csv_path = root_path+'/../inputfile'
    else:
        csv_path = sys.argv[1]
    
    if (not os.path.isfile(root_path+'/__JsonDataInfo.py')) or sys.argv[2] == 'n':
        with open(root_path+'/__JsonDataInfo.py', 'w') as f:
            json_info=dict()
            f.write('JsonDataInfo=%s\n' %json_info)

    file_list = []
    recursive_search_dir(csv_path, file_list)
    file_list = cleanup_filelist(file_list)

    import __JsonDataInfo
    json_info = __JsonDataInfo.JsonDataInfo
    print("\n추가된 파일 수 : " + str(len(file_list)))
    print("\n갱신중... 잠시만 기다려 주세요")
    cnt=0
    for file_path in file_list:
        cnt+=1
        with open(file_path,'r') as f:
            _list = json.loads(f.read())
        
        if len(_list[0]) > 4 :
            _list = _list[1:-1]
        if len(_list) == 0:
            continue
        file_name = file_path.split('/')[-1]
        print(str(cnt) + '. ' + file_name)
        file_name_part = file_name.split('_')
        file_name_part = file_name_part[0:-1]
        file_name=""
        for name_part in file_name_part:
            file_name = file_name + name_part + '_'
        file_name = file_name[0:-1]
        totalNum = len(_list)

        
        new_dict = {}
        new_dict[file_name] = {}
        new_dict[file_name]["files"] = []
        new_dict[file_name]["files"].append(file_path)
        new_dict[file_name]["metric"] = _list[0]["metric"]
        _list = sorted(_list, key=lambda k: k['timestamp'])
        new_dict[file_name]["minTS"] = str(_list[0]["timestamp"])
        new_dict[file_name]["maxTS"] = str(_list[totalNum-1]["timestamp"])
        new_dict[file_name]["totalValNum"] = totalNum
        _list = sorted(_list, key=lambda k: k['value'])
        new_dict[file_name]["minVal"] = _list[0]["value"]
        new_dict[file_name]["maxVal"] = _list[totalNum-1]["value"]
        new_dict[file_name]["tags"] = _list[0]["tags"]

        if file_name in json_info.keys():
            if int(json_info[file_name]["maxTS"]) < int(new_dict[file_name]["maxTS"]):
                json_info[file_name]["maxTS"] = new_dict[file_name]["maxTS"]
            if int(json_info[file_name]["minTS"]) > int(new_dict[file_name]["minTS"]):
                json_info[file_name]["minTS"] = new_dict[file_name]["minTS"]
            json_info[file_name]["totalValNum"] += new_dict[file_name]["totalValNum"]
            if json_info[file_name]["maxVal"] < new_dict[file_name]["maxVal"]:
                json_info[file_name]["maxVal"] = new_dict[file_name]["maxVal"]
            if json_info[file_name]["minVal"] > new_dict[file_name]["minVal"]:
                json_info[file_name]["minVal"] = new_dict[file_name]["minVal"]
            json_info[file_name]["files"].append(file_path)
        else:
            json_info[file_name] = new_dict[file_name]

    with open(root_path+'/__JsonDataInfo.py', 'w') as f2:
        f2.write('JsonDataInfo=%s\n' %json_info)
    
    print('\n\n------------JsonData 목록------------\n')
    cnt = 1
    for key in json_info.keys():
        print(str(cnt) + " : " + key)
        cnt+=1
    print('\n')
        
