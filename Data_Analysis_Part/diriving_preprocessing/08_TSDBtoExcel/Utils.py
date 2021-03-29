# -*- coding: utf-8 -*-
from __future__ import print_function

from scandir import scandir
import datetime
import time



separator = '/'
fmtlen = 0
fcounts = 0

def _recursive_search_all_generator(flist, cwd, fmt):
    '''
    Generator function for yielding all csv files in the target directory
        '''
    global fmtlen

    if len(flist) != 0:
        try:
            for entry in flist:
                if entry.is_dir(follow_symlinks = True):
                    _cwd = cwd + separator + entry.name
                    _flist = sorted(scandir(_cwd),key=(lambda entry: entry.name))
                    for i in _recursive_search_all_generator(_flist, _cwd, fmt):
                        yield i

                elif entry.is_file(follow_symlinks = False):
                    if entry.name[-fmtlen:] == fmt:
                        #_time = int(entry.name[-12:-4])
                        fpath = cwd + separator + entry.name
                        yield fpath
                            
                        

        except IOError as e:
            #permission
            if e.errno == errno.EPERM:
                print('PERMISSION ERROR')


def _recursive_search_by_time_generator(flist, cwd, fmt, bytime):
    '''
    Generator function for yielding all csv files in the target directory
        '''
    global fmtlen

    if len(flist) != 0:
        try:
            for entry in flist:
                if entry.is_dir(follow_symlinks = True):
                    _cwd = cwd + separator + entry.name
                    _flist = sorted(scandir(_cwd),key=(lambda entry: entry.name))
                    for i in _recursive_search_by_time_generator(_flist, _cwd, fmt, bytime):
                        yield i

                elif entry.is_file(follow_symlinks = False):
                    if entry.name[-fmtlen:] == fmt:
                        _time = int(entry.name[-12:-4])
                        if bytime['start'] <= _time and _time < bytime['end']:
                            fpath = cwd + separator + entry.name
                            yield fpath
                            
                        

        except IOError as e:
            #permission
            if e.errno == errno.EPERM:
                print('PERMISSION ERROR')

def _recursive_search_by_carid_generator(flist, cwd, fmt, bycarid):
    '''
    Generator function for yielding all csv files in the target directory
        '''
    global fmtlen

    if len(flist) != 0:
        try:
            for entry in flist:
                if entry.is_dir(follow_symlinks = True):
                    _cwd = cwd + separator + entry.name
                    _flist = sorted(scandir(_cwd),key=(lambda entry: entry.name))
                    for i in _recursive_search_by_carid_generator(_flist, _cwd, fmt, bycarid):
                        yield i

                elif entry.is_file(follow_symlinks = False):
                    if entry.name[-fmtlen:] == fmt:
                        if bycarid == int(entry.name[-23:-13]):
                            fpath = cwd + separator + entry.name
                            yield fpath
                        

        except IOError as e:
            #permission
            if e.errno == errno.EPERM:
                print('PERMISSION ERROR')

def recursive_search(target_directory, condition='ALL', condition_value=None, fmt='.csv'):
    '''
    Returns the generator of the list of all files in the target_directory

    Params:
    - target_directory : path of a directory
    - condition : time range condition dictionary
                  {'start':20160101(정수), 'end':20180101}
    - fmt : format of the target files (ex. '.csv', '.txt', ...)
        '''

    if type(fmt) != str :
        print('Wrong parameter type : fmt should be string')
        raise ValueError

    global fmtlen

    fmtlen = len(fmt)
    flist = sorted(scandir(target_directory),key=(lambda entry: entry.name))

    gen = None
    if condition == 'ALL' :
        gen = _recursive_search_all_generator(flist, target_directory, fmt)
    elif condition == 'BYTIME' :
        gen = _recursive_by_time_search_generator(flist, target_directory, fmt, condition_value)
    elif condition == 'BYCARID' :
        gen = _recursive_search_by_carid_generator(flist, target_directory, fmt, condition_value)

    return gen

def _get_counts(addr_target_dir, condition, condition_value, fmt):
    global fcounts
    
    for entry in sorted(scandir(addr_target_dir),key=(lambda entry: entry.name)):
        try:
            #entry가 디렉토리인 경우
            if entry.is_dir(follow_symlinks = True):
                _get_counts(addr_target_dir+separator+entry.name, condition, condition_value,fmt)

            #entry가 파일인 경우
            elif entry.is_file(follow_symlinks = False):
                if len(entry.name) < 5:
                    continue

                addr_entry = addr_target_dir + separator + entry.name
                if entry.name[-fmtlen:] == fmt:
                    if condition == 'ALL':
                        fcounts += 1
            
                    elif condition == 'BYCARID':
                        if condition_value == int(entry.name[-23:-13]):
                            fcounts += 1
                
                    elif condition == 'BYTIME':
                        if condition_value['start'] <= _time and _time < condition_value['end']:
                           fcounts += 1

        except IOError as e:
            #permission
            if e.errno == errno.EPERM:
                print('PERMISSION ERROR')

def get_counts(target_directory, condition='ALL', condition_value=None, fmt='.csv'):

    
    if type(fmt) != str :
        print('Wrong parameter type : fmt should be string')
        raise ValueError

    global fmtlen
    global fcounts

    fmtlen = len(fmt)
    fcounts = 0
    _get_counts(target_directory, condition, condition_value, fmt)
    

    return fcounts
    


def _timeTOunixtime (real_time):
    '''
    Returns unixtime of the real time
    Params:
    - real_time : real time in the following format (YYYY-MM-DD hh:mm:ss)
    '''


    try:
        stime = "%s/%s/%s" %(real_time[8:10], real_time[5:7], real_time[0:4])
        if int(stime[-4:]) < 1970 :
            return -1

        sec = int(real_time[11:13])*60*60 + int(real_time[14:16])*60 +int(real_time[17:19])

        unixday = time.mktime(datetime.datetime.strptime(stime, "%d/%m/%Y").timetuple())
        unixtime = unixday + sec
    except Exception:
        return -1

    return int(unixtime)


def strday_delta(_s, _type, _h_scale):
    _dt  = str2datetime(_s)
    if _type == 'days' :
        _dt += datetime.timedelta(days = _h_scale)
    elif _type == 'hrs':
        _dt += datetime.timedelta(hours = _h_scale)
    elif _type == 'mins':
        _dt += datetime.timedelta(minutes = _h_scale)
    #if _h_scale == 24 : _dt += datetime.timedelta(days = 1)
    #elif _h_scale == 1 : _dt += datetime.timedelta(hours = 1)
    #elif _h_scale == 0.1 : _dt += datetime.timedelta(minutes = 20)
    _str = datetime2str(_dt)
    return _str

def daydelta(_date_on, _date_off, dys=None, hrs=None, mins=None):

    _on = str2datetime(_date_on)
    _off = str2datetime(_date_off)
    
    if dys != None:
        _delta = _on + datetime.timedelta(days=1)
    elif hrs != None:
        _delta = _on + datetime.timedelta(hours=1)
    elif mins != None:
        _delta = _on + datetime.timedelta(minutes=20)

    if _delta == _off:
        return None
    else:
        return datetime2str(_delta)


def str2datetime(dt_str):
    return datetime.datetime.strptime(dt_str, "%Y/%m/%d-%H:%M:%S")


def datetime2str(dt):
    return dt.strftime('%Y/%m/%d-%H:%M:%S')

def unixtime2datetime(unixtime):
    _date_time = datetime.datetime.fromtimestamp(int(unixtime))
    _date = datetime.datetime.strftime(_date_time, '%Y/%m/%d-%H:%M:%S')

    return _date

