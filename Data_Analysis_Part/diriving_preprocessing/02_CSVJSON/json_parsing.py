#-*- coding:utf-8 -*-

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
    parser.add_argument("-rename_field", help="select key you'll change")
    
    args = parser.parse_args()
    
    if (args.debug) : 
        g_DEBUG = True
    
    return args

def recursive_search_dir(_nowDir, _filelist):
    f_list = os.listdir(_nowDir)
    dir_list=[]
    for fname in f_list:
        if os.path.isdir(_nowDir+'/'+fname):
            dir_list.append(_nowDir+'/'+fname)
        elif os.path.isfile(_nowDir+'/'+fname):
            _filelist.append(_nowDir+'/'+fname)
            return
    for toDir in dir_list:
        recursive_search_dir(toDir, _filelist)


if __name__ =='__main__':
    root_path = './originalCSVData/sw_make/resultData'
    file_list = []
    
    global g_DEBUG
    g_DEBUG = True

    _args_ = brush_argparse()
    input_dir = _args_.input_dir
    str_change = _args_.rename_field

    recursive_search_dir(input_dir, file_list)
    file_path = file_list[0]
    
    with open(file_path,'r') as f:
        _list = json.loads(f.read())
    
    _dict = _list[1]
    print('\n --- json example ---')
    pprint.pprint(_dict)
    print('\n')
