# -*- coding: utf-8 -*-
#!/usr/bin/env python2

from __future__ import print_function
import os
import sys
import time
import datetime
from collections import OrderedDict
import copy

# 호출되는 함수 
def prep(s_ut, e_ut, _dictbuf_list, _sec, meta):
    _ret = _idcnt(s_ut, e_ut, _dictbuf_list, _sec)
    #print ('  happy end')  
    return _ret 

def _idcnt(s_ut, e_ut, _dictbuf_list, _sec):

    '''
    COPY data, in fact, nothing done
    data 1 --> data 2
    - param s_ut: 쿼리한 데이터 시작시간
    - param e_ut: 쿼리한 데이터 종료시간
    - param _dictbuf_list: 하루단위로 쿼리하여 리턴된 데이터, dps package
    - param _sec: 데이터를 묶을 시간단위
    return: 새로 구성된 데이터
    - _dictbuf_list, check it on file './output.sample.txt'
      - 'dps': {u'1401672163': 1, ....}
      - u'tags': {u'content': u'exist_data', u'carid': u'588'}}
        '''
    
    _dict_buf_tmp = list(_dictbuf_list)

    print (_dict_buf_tmp)

    '''
    _now = datetime.datetime.now()
    _t = _now.strftime('%Y%m%d_%H-%M-%S')
    _t = _t[:-3]
    for i in range(len(_dictbuf_list)):
        _dict_buf_tmp[i]['tags']['copytime'] = _t
        '''
    return _dict_buf_tmp


# 반드시 실행 확인 할것. 현재 python 파일이 실행 오류나면,
# python 코드 실행중 import 해도 실행이 되지 않음
# 어려운 점은 에러가 보이지 않음

if __name__ == "__main__" : 

    print ("working good")