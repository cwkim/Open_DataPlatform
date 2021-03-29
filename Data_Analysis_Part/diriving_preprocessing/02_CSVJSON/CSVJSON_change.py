#!/usr/bin/env python
#-*- coding: utf-8 -*-

'''
    Author : Jaekyu Lee, github : https://github.com/JaekyuLee
             https://github.com/ChulseoungChae
             https://github.com/KimHyeongGoo
             https://github.com/jeonghoonkang
'''

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

''' printProgressBar() 함수 호출시 주석 해제 (DecodeError방지)
if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding("utf-8")
'''    
def dprint(s): # debug_print
    global g_DEBUG
    if (g_DEBUG):
        print ('  ', s)
    else : return None


def brush_argparse():
    
    global g_DEBUG # dprint 함수 실행을 위한 플래그

    parser = argparse.ArgumentParser()
    parser.add_argument("-debug", help="debug mode run", action="store_true")
    parser.add_argument("-input_dir", help="select directory with Json files")
    parser.add_argument("-output_dir", help="select directory to save Json")
    parser.add_argument("-rename_field", help="select key you'll change")
    parser.add_argument("-strold", help="write str to be changed")
    parser.add_argument("-strnew", help="write new str to be changed")

    args = parser.parse_args()
    
    if (args.debug) : 
        g_DEBUG = True
        dprint ('DPRINT Enabled ************************************** ' + __file__ )
    
    return args


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

def recursive_search_dir(_nowDir, _filelist):
    dir_list = [] # 현재 디렉토리의 서브디렉토리가 담길 list
    f_list = os.listdir(_nowDir)
    for fname in f_list:
        if os.path.isdir(_nowDir+'/'+fname):
            dir_list.append(_nowDir+'/'+fname)
        elif os.path.isfile(_nowDir+'/'+fname):
            _filelist.append(_nowDir+'/'+fname)

    for toDir in dir_list:
        recursive_search_dir(toDir, _filelist)

def debugPrint(_filename):
    # print content 
    with open(_filename, "rt") as fin: # current Directory(rt : read text mode)
        for line in fin:
            print(line)
            if('},' in line): # find char. if it is true, then break
                break

def txtLineCount(_file):
    lines = 0
    for line in open(str(_file)):
        lines += 1
    return lines  

def maketree(path, subdirlist, filename):
    for subdir in subdirlist:
        path += '/' + subdir
        if not os.path.isdir(path):
            os.makedirs(path)
    realfilename = filename.split('/')[-1]
    path+= '/' + realfilename
    return path

def changeJsonStr(_input_dir, _output_dir, _rename_field, _oldstr, _newstr):
    # to use progress bar
    filePerCount =0
    _oldstr = _oldstr.replace('@',' ')
    _newstr = _newstr.replace('@',' ')
    # Current Directory path
    #directory = os.path.dirname(os.path.realpath(__file__))

    path_dir = _output_dir
    file_list = []
    
    recursive_search_dir(_input_dir, file_list)

    #path_dir += '/changedData'
    if not os.path.isdir(path_dir):
        os.makedirs(path_dir)
    
    cnt=0
    for file_name in file_list:
        cnt+=1
        #print(file_name)
        
        print("\n\n")
        print("%s reading...\n" %file_name)
    
        lines = txtLineCount(file_name)
        print(file_name+" lines are : ", lines)

        # print content
        c=0
        with open(file_name, "rt") as fin:
            for line in fin:
                #print("Lines are"+line)
                print(line)
                if('},' in line): # find char. if it is true, then break
                    if c==0:
                        c=1
                    else:    
                        break    


        filePerCount=0
        # read file
        with open(file_name, "rb") as fin:
            subdir_list = file_name.split(_input_dir)[1]
            subdir_list = subdir_list.split('/')
            if len(subdir_list) == 1:
                file_name(path_dir, [], file_name)
            else:
                subdir_list = subdir_list[0:-1]
                file_name = maketree(path_dir, subdir_list, file_name)
            with open(file_name, "wt") as fout:

                json_data = copy.deepcopy(json.load(fin))
                for _data in json_data[1:]:
                    if _rename_field == 'key':
                        if _oldstr == 'timestamp' or _oldstr == 'metric' or _oldstr == 'value':
                            _data[_oldstr] = _data.pop(_newstr)
                        else:
                            _data['tags'][_newstr] = _data['tags'].pop(_oldstr)
                    elif _rename_field == 'value':
                        if _oldstr == 'timestamp' or _oldstr == 'metric' or _oldstr == 'value':
                            _data[_oldstr] = str(_newstr)
                        else:
                            _data['tags'][_oldstr] = str(_newstr)
                json.dump(json_data, fout, indent=4)

        #printProgressBar(cnt, len(file_list))
        #debugPrint('output/changedData/'+file_name.split('/')[-1]) # print debug message

if __name__ == '__main__':
    global g_DEBUG
    g_DEBUG = False

    _args_pack_ = brush_argparse()
    input_dir  = _args_pack_.input_dir
    output_dir = _args_pack_.output_dir
    str_change = _args_pack_.rename_field
    old_str = _args_pack_.strold
    new_str = _args_pack_.strnew

    changeJsonStr(input_dir, output_dir, str_change, old_str, new_str)
    print('\n')