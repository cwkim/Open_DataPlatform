#-*- coding: utf-8 -*-
#!/usr/bin/env python

from __future__ import print_function
import pandas as pd
import time
import sys
import os
import csv
import io
import shutil

# 출력 내용이 저장될 텍스트 파일
inputfile = open('_sw_make_input_.txt', 'w')

# 기존에 기록된 type_file.py의 파일 목록중 삭제되거나 수정된 파일 갱신
def renewFileTypeDir(_rootdir):
    global inputfile
    oldfilelist=[]
    
    import type_file
    file_type = type_file.file_type
    dir_def = type_file.dir_inf
    file_type_len = len(file_type)
    
    idx=1
    print("\n갱신되거나(삭제 또는 수정된) 원본 디렉토리에 존재하지 않는 파일 목록\n")
    inputfile.write("\n갱신되거나(삭제 또는 수정된) 원본 디렉토리에 존재하지 않는 파일 목록\n\n")
    # 기존에 기록된 파일이 존재하지 않으면 또는 수정된 파일이 있으면 갱신
    # 입력된 원본 csv파일 디렉토리에 포함되지 않는 파일 갱신 
    while True:
        key = 'type_'+str(idx)
        if key not in file_type.keys():
            break
        filetypedict = file_type[key]['files']
        filepath_list=list(filetypedict.keys())
        for filepath in filepath_list:
            # 파일이 없거나 파일 수정시간이 변경되었을시 또는 입력된 원본 csv파일 디렉토리에 포함되지 않을 경우
            if not os.path.isfile(filepath) or time.ctime(os.path.getmtime(filepath)) != filetypedict[filepath] or not filepath.startswith(_rootdir):
                print(filepath)
                inputfile.write(filepath + '\n')
                del filetypedict[filepath]
                k2 = filepath.split('/')[-1]
                k1 = filepath.replace('/'+k2,'')
                del dir_def[k1][k2]
                if len(dir_def[k1]) == 0:
                    del dir_def[k1]
  
        file_type[key]['files']=filetypedict
        
        if len(filetypedict)==0:
            del file_type[key]
        else :
            oldfilelist = oldfilelist+list(filetypedict.keys())
        idx+=1
    
    # type_file.py 의 file_type 딕셔너리 키 이름 순서대로 갱신
    for index in range(len(file_type)):
        index+=1
        if 'type_'+str(index) in file_type.keys():
            None
        else :
            i=index+1
            while True:
                if i > file_type_len:
                    break
                if 'type_'+str(i) in file_type.keys():
                    file_type['type_'+str(index)] = file_type.pop('type_'+str(i))
                    break
                i+=1
    
    print('\n')
    inputfile.write('\n')

    with open('type_file.py', 'w') as f:
        f.write('file_type=%s\n' %file_type)
        f.write('dir_inf=%s\n' %dir_def)

    # filetype에 기록된 파일리스트(기존에 분류된 파일) 반환
        # 이 파일 리스트로 이후 실행될 서브디렉토리 탐색에서 새로 추가된 파일을 찾음 

    return oldfilelist

def recursive_search_dir(_nowDir, _filelist, _type, _oldfilelist):
    global inputfile
    if _type ==  'csv' : # csv
        _idx = -3
    elif _type == 'xlsx' : # xlsx
        _idx = -4
    dir_list = [] # 현재 디렉토리의 서브디렉토리가 담길 list
    f_list = os.listdir(_nowDir)
    print (" [loop] recursive searching ", _nowDir)
    inputfile.write(" [loop] recursive searching " + _nowDir + '\n')
    
    for fname in f_list:
        if os.path.isdir(_nowDir+'/'+fname):
            if fname.startswith('output'):
                continue
            dir_list.append(_nowDir+'/'+fname)
        elif os.path.isfile(_nowDir+'/'+fname):
            if fname[_idx:] == _type:
                if _nowDir+'/'+fname not in _oldfilelist:
                    _filelist.append(_nowDir+'/'+fname)

    for toDir in dir_list:
        recursive_search_dir(toDir, _filelist, _type, _oldfilelist)

def csv_to_chunk(_file, _encoding):
    chunks = pd.read_csv(_file, low_memory=False, chunksize=10000, encoding=_encoding)
    dflen = 0
    columns_list = []
    for chunk in chunks:
        if dflen==0:
            for col in chunk.columns:
                columns_list.append(col)
        dflen+=len(chunk)
        
    return dflen, columns_list
        
def _get_fieldlist(_filelist, _type):
    _idx = -3
    columns_type_list=[]

    if _type ==  'csv' : # csv
        None
    elif _type == 'xlsx' : # xlsx
        _idx = -4

    import type_file
    file_type = type_file.file_type
    dir_info = type_file.dir_inf
    print("추가된 csv 파일 정보 추출중..\n")
    
    for _it in _filelist :
        if _it[_idx:] == _type :
            if _type == 'csv': 
                try:
                    dflen,columns_list = csv_to_chunk(_it, 'utf-8')
                except UnicodeDecodeError:
                    try:
                        dflen,columns_list = csv_to_chunk(_it, 'euc-kr')
                    except UnicodeDecodeError:
                        dflen,columns_list = csv_to_chunk(_it, 'cp949')

            if dflen==0 or len(columns_list)==0:
                continue

            # directory & file definition
            file_row_len = dflen # line수
            file_name = _it.split('/') # file name
            file_path = '/'.join(file_name[0:-1])
            file_name = file_name[-1]
            if file_path not in dir_info:
                dir_info[file_path]={}
            dir_info[file_path][file_name] = file_row_len
            
            # classify file_type
            idx=0
            for keys in file_type.keys():
                if sorted(file_type[keys]['columns']) == sorted(columns_list):
                    file_type[keys]['files'][_it] = time.ctime(os.path.getmtime(_it))
                    break
                idx+=1

            if idx == len(file_type):
                new_type = dict()
                new_type['columns'] = columns_list
                new_type['files'] = dict()
                new_type['files'][_it] = time.ctime(os.path.getmtime(_it))
                file_type['type_'+str(idx+1)]=new_type


 
    ### add ###
    f = open('type_file.py', 'w')
    f.write('file_type=%s\n' %file_type)
    f.write('dir_inf=%s\n' %dir_info)
    f.close()

def print_fields():
    global inputfile
    print('\n--------------------------디렉토리 정보---------------------------\n')
    inputfile.write('\n--------------------------디렉토리 정보---------------------------\n\n')
    dir_def = type_file.dir_inf
    for dirpath in dir_def.keys():
        print(dirpath)
        inputfile.write(dirpath+'\n')
        total = 0
        for fname in dir_def[dirpath].keys():
            print(fname + ' : ' + str(dir_def[dirpath][fname]) + ' lines')
            inputfile.write(fname + ' : ' + str(dir_def[dirpath][fname]) + ' lines\n')
            total += dir_def[dirpath][fname]
        print('total : ' + str(total) + ' lines\n')
        inputfile.write('total : ' + str(total) + ' lines\n\n')
    print('\n----------------------------필드 목록-----------------------------')
    inputfile.write('\n----------------------------필드 목록-----------------------------\n')
    file_type = type_file.file_type
    for num in range(len(file_type)):
        print('\n'+str(num+1)+'번째 type file : ' + str(len(file_type['type_'+str(num+1)]['files'])))
        inputfile.write('\n'+str(num+1)+'번째 type file : ' + str(len(file_type['type_'+str(num+1)]['files']))+'\n')
        for i in range(len(file_type['type_'+str(num+1)]['columns'])):
            print("%s%s" %(i,file_type['type_'+str(num+1)]['columns'][i].center(30)))
            inputfile.write("%s%s\n" %(i,(file_type['type_'+str(num+1)]['columns'][i]).encode('utf-8').center(30)))
        print(' ')
        inputfile.write('\n')
    print('\n------------------------------------------------------------------')
    inputfile.write('\n------------------------------------------------------------------\n')

if __name__ == '__main__':
    saved_type_file_name = 'type_file.py'
 
    # file type을 관리할 파이썬파일 생성
    if os.path.isfile(saved_type_file_name) is False:
        with open('type_file.py', 'w') as f:
            file_type=dict()
            dir_def=dict()
            f.write('file_type=%s\n' %file_type)
            f.write('dir_inf=%s\n' %dir_def)

    root_path = sys.argv[2]
    if root_path[-1] == '/':
        root_path = root_path[0:-1]
    
    if not os.path.isdir(root_path):
        print("올바르지 않는 원본 CSV 파일 directory 경로입니다.\n")
        exit()
    
    # 삭제된 파일 갱신
    oldfile_list = renewFileTypeDir(root_path)
        
    file_list = [] # 발견된 file이 담길 list
    filetype = 'csv'
    if len(sys.argv) == 2:
        filetype = sys.argv[1]
    # 새로 추가된 파일 탐색
    print("CSV 파일 탐색 시작\n")
    inputfile.write("CSV 파일 탐색 시작\n\n")
    recursive_search_dir(root_path, file_list, filetype, oldfile_list)
    print( "\n갱신될(추가된) 파일 목록 수 (개): " + str(len(file_list)) + '\n')
    inputfile.write("\n갱신될(추가된) 파일 목록 수 (개): " + str(len(file_list)) + '\n\n')
    # 새로 추가된 파일 갱신
    if len(file_list)>0:
        _get_fieldlist(file_list, filetype)
    # 필드 목록 출력
    if os.path.isfile(saved_type_file_name) is True:
        import type_file
        print_fields()
