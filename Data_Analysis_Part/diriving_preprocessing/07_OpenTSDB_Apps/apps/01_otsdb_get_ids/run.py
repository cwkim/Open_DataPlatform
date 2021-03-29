# -*- coding: utf-8 -*-

# Author : https://github.com/jeonghoonkang
# version 1.1
# 버전 주요 변경 사항 : OpenTSDB 읽기를 위한 time unit , time long 아규먼트 추가

# python library
from __future__ import print_function
import os
import sys
import time
import importlib

# 개발 코드 import
print ('==>', __file__, "importing private modules, using relative PATH" )

sys.path.append("../../lib/preprocessing")
sys.path.append('../../lib/multi_proc')
sys.path.append('../../lib/opentsdb')
sys.path.append('../../lib/log')
sys.path.append('../../lib')

import Preprocessing
import query_drivers as qd



def brush_args():
    '''
    - 사용자 입력 인자값 읽는함수
    {read 아이피주소} {read 포트번호} {read 메트릭이름} {write 아이피주소} {write 포트} 
    {write 메트릭이름} {시작시간} {종료시간} {묶을 단위시간} 
    {아이디처리방법} {content type(dp 태그)} {aggregator} {ID} {파일이름} {프로듀서 proc 갯수} {컨슈머 proc 갯수}
        '''
    usage = "\n usage : python %s {IP address} {port} " % sys.argv[0]
    usage += "{in_metric_name} {out_ip} {out_port} {out_metric} {start_day} {end_day} {partition_sec} {processingtype} {content type} {aggregator} {carid}"
    print(usage + '\n')

    _len = len(sys.argv)

    if _len < 18:
        print(" 추가 정보를 입력해 주세요. 위 usage 설명을 참고해 주십시오")
        print(" python 실행파일을 포함하여 아규먼트 갯수 18개 필요 ")
        print(" 현재 아규먼트 %s 개가 입력되었습니다." % (_len))
        print(" check *run.sh* file ")
        exit(" You need to input more arguments, please try again \n")

    _ip = sys.argv[1]
    _port = sys.argv[2]
    _in_met = sys.argv[3]
    _out_ip = sys.argv[4]
    _out_port = sys.argv[5]
    _out_met = sys.argv[6]

    _start = sys.argv[7]
    _end = sys.argv[8]
    _seconds = sys.argv[9] #use??
    _proctype = sys.argv[10]
    _content = sys.argv[11]

    _aggregator = sys.argv[12]
    _carid = sys.argv[13]
    _filename = sys.argv[14]
    _pn = sys.argv[15]
    _cn = sys.argv[16]
    _timeunit = sys.argv[17]
    _timelong = sys.argv[18]

    # check 디버깅용 프린트
    # for _ix in range(18): print ('[',_ix,']', sys.argv[_ix])


    if (len(_start) < 15 or len(_end) < 15):
        _start = ketidatetime._check_time_len(_start)
        _end = ketidatetime._check_time_len(_end)

    #_sort 제외
    return _ip, _port, _in_met, _out_ip, _out_port, \
        _out_met, _start, _end, _seconds, _proctype, \
        _content, _aggregator, _carid, _filename, _pn, _cn, \
        _timeunit, _timelong

def savefile(buf):
    f = open('save_id_list.py', 'w')    # 경로설정 / 덮어쓰기
    f.write('car_id_list=%s\n' % buf[0])
    ###################### add ######################
    f.write('dps_num_dict=%s\n' % buf[1])
    f.write('dps_per_dict=%s\n' % buf[2])
    f.write('period_len_num=%s\n' % buf[3])
    f.write('sorted_dps_num=%s\n' % buf[4])
    f.write('sorted_dps_per=%s\n' % buf[5])
    f.write('non_exist_dps_num_dict=%s\n' % buf[6])
    f.write('non_exist_dps_per_dict=%s\n' % buf[7])
    f.write('grouped_dict=%s' % buf[8])
    ###################### /add ######################
    f.close()


if __name__ == "__main__":
    
    # 사용자가 입력한 인자값들을 읽어 딕셔너리 형태로 저장
    ip, port, in_metric, out_url, out_port, \
    out_metric, q_start, q_end, seconds, processingtype, \
    content, aggregator, carid, fname, pn, \
    cn, timeunit, timelong = brush_args()

    meta = dict()  #메타정보 저장할 딕셔너리 생성

    meta['ip'] = ip  #read 아이피주소
    meta['port'] = port  #read포트번호
    meta['in_metric'] = in_metric  #read메트릭이름
    meta['out_url'] = out_url  #write 아이피주소
    meta['out_port'] = out_port  #write 포트
    meta['out_metric'] = out_metric

    # out metric 은 입력되는 상황변수에 따라서 변경하여 사용해야 함 
    if  meta['out_metric'] != 'none':
        meta['out_metric'] = out_metric + q_start[:10] + '_' + q_end[:10]  #write 메트릭이름
    
    meta['query_start'] = q_start  #시작시간
    meta['query_end'] = q_end  #종료시간
    meta['seconds'] = seconds  #묶을 단위시간
    
    meta['processingtype'] = processingtype # 아이디 처리 방식
    if content == 'none':     # shell 에서 '*'를 입력할수 없어서, shell 에서 입력된 'none' 값이 이곳까지 적용
        meta['content'] = '*' # 여기서 비교를 통해 '*' 로 수정
    else:
        meta['content'] = content

    # 특정 tags를 입력해야 하는 경우, fname 함수 내부 변수인 tags에 할당된 값을 받아옴
    # 특정 tags 없으면 pass 
    try:
        ifunc = importlib.import_module(fname)
        print (ifunc)
        try:
            if ifunc.tags:
                meta['content'] = ifunc.tags
        except:
            pass
    except :
        print (" exception : fail to import at", __file__, fname)
        print(sys.modules)
        print(sys.path)
        exit()
    
    meta['aggregator'] = aggregator
    
    if carid == 'none':  carid = '*'    
    else :
        if meta['out_metric'] != 'none': meta['out_metric'] += '_id_' + ('%4d'%(carid))
        # 이후, carid 의 숫자를 항상 4자리로 만들면 좋겠음
        # meta['out_metric'] 이 'none' 이면, 이후에 tsdb write를 하지 않겠다는 의미임
        # multi proc에서 write를 하지 않음

    # check out metric name change
    #print (meta['out_metric'])
    #exit()

    meta['carid'] = carid
    

    meta['importname'] = fname    
    meta['pn'] = pn
    meta['cn'] = cn

    meta['timeunit'] = timeunit
    meta['timelong'] = timelong

    # 입력 아규먼트는 아니지만 추후 사용을 위해 입력 
    #meta['carid'] = "*" #  사용시 ex) meta['carid'] = "296|508|322"
    print( '[ OUT METRIC NAME ]' , '@'+__file__, meta['out_metric'] )
    
    # 여기서 시간 loop 에 사용할 변수 정리
    # 처리할 시간단위, 정리
    meta['days'] = None
    meta['hrs'] = None
    meta['mins'] = None

    _tlong = meta['timelong']
    #print (_tlong)

    _tlong = int(_tlong, base=10)
    if meta['timeunit'] == 'd' :
        meta['days'] = _tlong
        if _tlong > 8 : exit("It's too long days : " + __file__ )

    elif meta['timeunit'] == 'h' :
        meta['hrs'] = _tlong
        if _tlong > 24 : exit("It's too long hours : " + __file__ )

    elif meta['timeunit'] == 'm' :
        meta['mins'] = _tlong
        if _tlong > 60 : exit("It's too long minnutes : " + __file__ )

    #print (meta)
    #exit()

    st = time.time()

    #if meta['works'] == 'num':
        
    ### Link to next step
    #qd.query_nto_tsdb(meta)
    ret_buf = qd.query_to_getids(meta)
    # get id 를 위한 지정 함수 실행
    #print (ret_buf)
    print ('len of list  --- ' + str(len(ret_buf)))
    
    savefile(ret_buf)
    # query_drivers.py의 query_nto_tsdb함수에 위에서 사용자가 입력한 meta정보 전달하여 실행

    et = time.time()
    print('Time elapsed : ',et-st)
    # 프로그램 동작 시간 출력