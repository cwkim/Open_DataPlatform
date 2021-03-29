# -*- coding: utf-8 -*-
# Author : ChulseoungChae , https://github.com/ChulseoungChae

from __future__ import print_function
import os
import time
import datetime
import sys
import pprint
import importlib


if __name__ == '__main__':
    var_file_name = '__var.py'

    if os.path.isfile(var_file_name) == True:
        import_file = importlib.import_module(var_file_name[:-3])
        meta = import_file.meta
        print('이전에 사용한 아규먼트 정보 : \n')
        pprint.pprint(meta, indent=4)
        print('\n')
    else:
        print('이전에 저장된 아규먼트 정보가 없습니다. 아래 정보들을 입력해주세요.\n')    
