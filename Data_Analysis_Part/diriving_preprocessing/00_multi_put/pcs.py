# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import requests
import time
import json
import sys
import multiprocessing
import keti_multiprocessing


def _consume(works_to_do, url):
   #Consumer function for sending preprocessed data to tsdb
    #(Never called directly)
    #Params:
    #- works_to_do 큐 (in_queue) : queue object for getting a json data
    #- to_Parent 큐 (out_queue) : queue object for sending a finish msg to Parent Process
    
    pid = str(os.getpid())
    
    try:
        sess = requests.Session()
        headers = {'content-type': 'application/json'}

    except requests.ConnectTimeout as e:
        print('ERROR : cannot connect to the server')
        exit()

    _wcnt = 0
    # get an item from queue(in_queue, works_to_do) and post it to tsdb
    while True:
        _wcnt += 1
        
        while works_to_do.empty():
            time.sleep(2)
        
        item = works_to_do.get()
        
        _qsz = works_to_do.qsize()
        if (_qsz % 250) == 0 :
            _pout = '  Queue Size %d     ||| \r' %(_qsz)
            sys.stdout.write(_pout)
            sys.stdout.flush()

        # work end
        if item == 'END':
            print("서브 프로세스(" + str(pid) + ") 작업 완료!")
            break
            
        else:
            try:
                response = sess.post(url, data=json.dumps(item), headers=headers)
    
                tries = 0
                # 서버로부터 응답이 오긴 하는데 ACK가 아닌 경우
                while (response.status_code > 204):
                    # try to change  50 dps -> 30 dps
                    print ( 'http put error', response, __file__)
                    print("[Process ID] " + pid)
                    
                    if tries > 10:
                        print('Tried count exceed')
                        tries = 0
                        break

                    time.sleep(0.05 * tries)
                    response = sess.post(url, data=json.dumps(item), headers=headers)  # , timeout=10)
                    tries += 1
                    
            # 서버로부터 아예 응답이 없는 경우
            except requests.ReadTimeout as e:
                retry = 0
                while True:
                    if retry > 10:
                        print('ERROR : NO RESPONSE FROM SERVER. EXITING THIS PROGRAM')
                        exit()
                    try:
                        sess = requests.Session()
                        break
                    except requests.ConnectTimeout as e:
                        retry += 1
                        time.sleep(10)
            
            # 기타 IO 관련 에러
            # 체크할수 있는 플래그가 있어야 하지만, 아직 구현 못함
            # 현재는 Queue max 갯수를 full 로 체크하다가, full 되면 종료시켜 버림
            except IOError as e:
                #shx.value += 1
                print( '#'*3, __file__, 'ERROR : IOError. system exits')
                # to do : 이 경우, 지속적으로 Query 하는 것을 막아줘야 함 
                exit()

    return True


JOBS = {'consume': _consume}


class Workers:
    '''
    입력을 전송하는 프로세스 관리 클래스
    일반적은 호출 순서는 다음과 같다:
    1. 객체 생성 (__init__)
    2. start_work() : 작업내용 및 정보 제공 (내부적으로 프로세스 생성)
    '''

    def __init__(self, nC, _url):
        '''
        Params:
        - nC : 컨슈머(전송) 프로세스 갯수 개수
        - _url : db 서버의 주소
            '''
               
        url=_url+'/api/put'
        print(nC, url)
        self.nC = nC

        # 데이터 송수신 큐 설정
        self.cwork_basket = None
        if nC != 0:
            self.cwork_basket = multiprocessing.Manager().Queue(maxsize=600000)
            # maxsize는 preproccessor의 iteration에 영향을 줌
            # (1일 기준) 86400초 * 600대, 50초(개) 묶음, 150 Byte 정도로 계산함
        print('work queue size : ' + str(self.cwork_basket.qsize()))
        
        self.works_done_location = url

        #프로세스간 공유를 위한 변수 i 정수 / d 더블프리시전 
        #공유변수 i 는 process 생성할때, 같이 해야함
        self.shared_x = multiprocessing.Value('i', 0)


    def start_work(self, job2='consume'):
        '''
         프로듀서에게 할 일(preprocessor)과 관련 메타데이터를 전달하여 전처리를 수행하도록 지시
         Params:
         - job2 : consumer process가 실행할 작업 (tsdb로 전송 / 그래프로 저장)
            '''

        # print (self.shared_x, self.shared_x.value)
        # 컨슈머 생성
        if self.nC != 0:
            self.consumers = keti_multiprocessing.mproc(self.nC, 'task2-worker')
        '''
        # 멀티프로세싱의 문제는 코드가 에러가 있을경우, 아무런 출력도 화면에 나오지 않아서
        # 마치 잘 동작하는것으로 착각을 일으킬 때가 있음
        # 아래 2줄로 코드가 제대로 동작하는지 확인하면 좋음
        # 확인 또 확인은 귀찮지만 전체 소요 시간을 줄여줄것이라 믿음
        # print (globals())
        # exit()
        
        # 특히, 새로운 process 생성할 때 에러가 많은데,
        # 미리 여기서 테스트해보고 apply 에 들어가는 패러미터를 결정하는 것이 좋아 보임
        # print ('address = ', self.shared_x)
        # print ('value = ', self.shared_x.value)
        
        # 프로세스 시작 위에서 nC 로 생성한 오브젝트에 실제 동작할 함수를 map 해주는 과정임
        # apply() => user code
            '''
        
        if self.nC != 0:
        #   self.consumers.apply(JOBS[job2], (self.cwork_basket, self.works_done_location, self.shared_x))
            self.consumers.apply(JOBS[job2], self.cwork_basket, self.works_done_location)

        '''
         consumers.apply()
         consumers는 class Workers의 __init__함수에서 keti_multiprocessing.mproc에서 함수 실행을 병렬화 선언한다
         cwork_basket은 class Workers의 __init__함수에서 queue로 설정해줬다
         apply 함수 : apply(function, (args))는 함수 이름과 그 함수의 인수를 입력으로 받아 간접적으로 함수를 실행시키는 명령어
            ex) apply(sum, (3,4)) <- 인수 3과 4를 입력으로 받아 sum함수를 실행
                출력 : 7
         프로듀서 프로세스 갯수가 0이 아닐때 
         인수[cwork_basket, works_done_location(Workers 클래스의 __init__함수에서 정의된 url='http://125.140.110.217:54242/api/put?'),meta]
         을 입력으로 받아서 JOBS[job2] = _consume 함수(이 코드의 line40)에 전달하여 실행시킨다
         따라서 consumers를 병렬화 선언후 _consume에 cwork_basket queue와 url, 메타정보를 인자로 주어 실행시키는 것이다 
            '''
        print('Workers-start success')
        
        return self.cwork_basket
    
    def report(self):
        '''
        프로듀서와 컨슈머 프로세스로부터 결과 리턴
        내부적으로 멀티프로세스 pool을 close, join함
            '''
        
        for idx in range(self.nC):
            self.cwork_basket.put('END')

        if self.nC != 0:
            # except 없이 수행이 잘 되면 제대로 return
            self.consumers.get()

  
