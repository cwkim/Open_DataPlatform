#!/usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import print_function
import os
import xlrd
import csv
import pandas as pd
import sys
import time
from multiprocessing import Pool

def printProgressBar(iteration, total, prefix = 'Progress', suffix = 'Complete',\
                      decimals = 1, length = 70, fill = '█'): 
    # 작업의 진행상황을 표시
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    _string_out = '\r%s |%s| %s%% %s' %(prefix, bar, percent, suffix)
    sys.stdout.write(_string_out)
    sys.stdout.flush()
    if iteration == total:
        None

def recursive_search_dir(_nowDir, _filelist):
    f_list = os.listdir(_nowDir)
    dir_list=[]
    total_size = 0
    for fname in f_list:
        if os.path.isdir(_nowDir+'/'+fname):
            dir_list.append(_nowDir+'/'+fname)
        elif os.path.isfile(_nowDir+'/'+fname):
            if fname[-4:] == 'xlsx':
                _filelist.append(_nowDir+'/'+fname)
                total_size += os.path.getsize(_nowDir+'/'+fname)
                
    for toDir in dir_list:
        total_size += recursive_search_dir(toDir, _filelist)
    
    return total_size
        

def csv_from_excel(xlsx_file):
    df = pd.read_excel(xlsx_file)
    filter_col = [col for col in df if col.startswith('Unnamed:')]
    df.drop(filter_col, axis=1, inplace=True)
    xlsx_file = xlsx_file.split(os.path.dirname(os.path.realpath(__file__))+'/')[-1]
    csv_file = 'csvoutput/'+xlsx_file[0:-4] + 'csv'

    while True:
        try:
            df.to_csv(csv_file, mode='w', encoding='utf-8')
            print(csv_file + " file converted!!\n")
            break
        # 디렉토리분리 (이름이 같은 파일 덮어쓰기 방지)
        except IOError:
            dir_l = csv_file.split('/')
            dirpath=''
            for dirname in dir_l:
                if dirname == dir_l[-1]:
                    break
                dirpath = dirpath+dirname+'/'
                if not os.path.isdir(dirpath):
                    os.makedirs(dirpath)
    
    
if __name__ == '__main__' :
    root_path = os.path.dirname(os.path.realpath(__file__))
    
    xlsx_list = []
    total_file_size = recursive_search_dir(root_path, xlsx_list)
    total_file_cnt = 0
    total_file_cnt=len(xlsx_list)
    
    print("total file size : "+str(total_file_size/1024)+' KB')
    print("total file cnt : "+str(total_file_cnt)+'\n')
    
    starttime = time.time()
    pool = Pool(5)
    pool.map(csv_from_excel, xlsx_list)        
    pool.close()      
    endtime = time.time()
    print('\n\nruntime : '+str(endtime-starttime))
