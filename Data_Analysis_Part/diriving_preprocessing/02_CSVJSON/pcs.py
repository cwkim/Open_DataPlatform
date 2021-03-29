# -*- coding: utf-8 -*-

# Author : https://github.com/CHOBO1
#          https://github.com/jeonghoonkang          

from __future__ import print_function
import os
import requests
import time
import json
import multiprocessing
import sys
from collections import OrderedDict
# from pathos.helpers import mp
# from pathos.multiprocessing import ProcessingPool as Pool

# 개발코드 import
import keti_multiprocessing
import CSVJSON_main

# df -> json buffer 
def _produce(works_to_do_list, qidx, works_done_list, meta):
    print('Producer %d Work Start\n' %os.getpid())
    works_to_do = works_to_do_list[qidx]
    totallines=0
    count=0
    while True:
        while (works_to_do.empty()):
            _pout = ' 프로듀서 큐 Producer queue is empty, wait for 1 \n'
            _pout = __file__ + _pout 
            #sys.stdout.write(_pout)
            #sys.stdout.flush()
            time.sleep (1)

        _pout = ' 프로듀서 큐 Producer queue has data packet  ___________________\n'
        _pout = __file__ + _pout 
        #sys.stdout.write(_pout)
        #sys.stdout.flush()

        #print (help('modules'))

        works = works_to_do.get()  # fpath : csv file path or OpenTSDB return JSON
        # work종료 메시지를 받으면 종료
        if works == 'END':
            print('Producer %d 종료\n' %os.getpid())
            break
        _list = works[0]
        json_title = works[1]
        _filename = works[2]

        Car_id = str(_list[meta['carid'].decode('utf-8')].iloc[0])
                
        dftime = _list[meta['timestamp'].decode('utf-8')].tolist()
        dfval = _list[meta['field'].decode('utf-8')].tolist()
        data_len = len(_list)
        totallines+=data_len
        _buffer = []

        for i in range(len(dftime)):
            value = dfval[i]
        
            # skip NaN value & ts
            if str(value) == 'nan' or str(dftime[i]) == 'nan':
                continue
            elif str(value) == 'NaN' or str(dftime[i]) == 'NaN':
                continue
            ts = CSVJSON_main.convertTimeToEpoch(dftime[i])
            ts = str(ts)
            csv_data = dict()
            csv_data['metric'] = meta['metric']
            csv_data["tags"] = dict()

            csv_data['timestamp'] = ts
            csv_data["value"] = value
    
            csv_data["tags"]['VEHICLE_NUM'] = Car_id
            if type(meta['field']) == type(b''):
                csv_data["tags"]["fieldname"] =  meta['field'].decode('utf-8')
            else:
                csv_data["tags"]["fieldname"] = meta['field']

            _buffer.append(csv_data)
        #print(len(_buffer))
        toidx = count % meta['cn']
        while (works_done_list[toidx].full()):
            time.sleep(0.5)
        works_done_list[toidx].put([_buffer, json_title, _filename, Car_id])
        count+=1
    
    return totallines


# json buffer -> json file
def _consume(works_to_do_list, qidx, meta, _empty):
    print('Consumer %d Work Start\n' %os.getpid())
    works_to_do = works_to_do_list[qidx]
    check=False
    while True:
        _buffer=[]
        json_title = ''
        for _ in range(meta['pn']):
            while (works_to_do.empty()):
                _pout = ' 컨서머 큐 Consumer queue is empty, wait for 1 \n'
                _pout = __file__ + _pout 
                #sys.stdout.write(_pout)
                #sys.stdout.flush()
                time.sleep (1)
            works = works_to_do.get()
            #print('Consumer %d get works\n' %os.getpid())

            # 종료메시지를 받으면 종료
            if works == 'END':
                check=True
                break

            _buffer = _buffer + works[0]
            json_title = works[1]
            _filename = works[2]
            carid = works[3]

        # 종료
        if check:
            print('Consumer %d 종료\n' %os.getpid())
            break
        
        _buffer = sorted(_buffer, key=lambda k: k['timestamp'])
        total_len = len(_buffer)
        #print(total_len)
        # 버퍼의 내용 파일로 저장
        s = 0
        e = s + meta['bundle']
        num=1
        while e <= total_len:
            put_buffer = _buffer[s:e]
            dataInfo={}
            CSVJSON_main.initDataInfo(dataInfo, put_buffer)
            dataInfo = OrderedDict(sorted(dataInfo.items(), key=lambda t: t[0]))
            put_buffer.insert(0, dataInfo)
            CSVJSON_main.writeJsonfiles(put_buffer, json_title, num, _filename, carid, meta['input_dir'], meta['outdir']) #save files by bundle
            s = e
            e = s + meta['bundle']
            num+=1

        # 버퍼에 남은 데이터 json 파일 생성
        if s < total_len:
            put_buffer = _buffer[s:total_len]
            dataInfo={}
            CSVJSON_main.initDataInfo(dataInfo, put_buffer)
            dataInfo = OrderedDict(sorted(dataInfo.items(), key=lambda t: t[0]))
            put_buffer.insert(0, dataInfo)
            CSVJSON_main.writeJsonfiles(put_buffer, json_title, num, _filename, carid, meta['input_dir'], meta['outdir']) #save files by bundle

    return True

class Workers:
    '''
    프로세스 관리 클래스

    일반적은 호출 순서는 다음과 같다:
    1. 객체 생성 (__init__)
    2. start_work() : 작업내용 및 정보 제공 (내부적으로 프로세스 생성)
    3. end_work() : 작업 마침표 전송
    4. report() : 프로세스 풀을 닫음

        '''

    def __init__(self, nP, nC):
         
        self.nP = nP
        self.nC = nC

        if nP == 0:
            raise AttributeError('nP should be at least 1')

        # 데이터 송수신 큐 설정
        self.pwork_basket_list=[]
        for _ in range(self.nP):
            self.pwork_basket_list.append(multiprocessing.Manager().Queue())
        self.cwork_basket_list=[]
        for _ in range(self.nC):
            self.cwork_basket_list.append(multiprocessing.Manager().Queue())
        
        
    def start_work(self, meta):

        # 프로듀서, 컨슈머 생성
        self.producers = keti_multiprocessing.mproc(self.nP, 'task1-worker')
        if self.nC != 0:
            self.consumers = keti_multiprocessing.mproc(self.nC, 'task2-worker')

        if self.nP != 0:
            # self.producers 는 mproc 클래스 
            self.producers.apply(_produce, self.pwork_basket_list, self.cwork_basket_list, \
                                 meta)

        if self.nC != 0:
           self.consumers.apply(_consume, self.cwork_basket_list, meta, None)

        print('Workers-start success')

        return (self.pwork_basket_list)


    def report(self):
        '''
        프로듀서와 컨슈머 프로세스로부터 결과 리턴
        내부적으로 멀티프로세스 pool을 close, join함
            '''
        for idx in range(self.nP):
            self.pwork_basket_list[idx].put('END')

        if self.nP != 0:
            # except 없이 수행이 잘 되면 제대로 return
            retlist = self.producers.get()

        for idx in range(self.nC):
            self.cwork_basket_list[idx].put('END')

        if self.nC != 0:
            # except 없이 수행이 잘 되면 제대로 return
            self.consumers.get()

        return retlist
