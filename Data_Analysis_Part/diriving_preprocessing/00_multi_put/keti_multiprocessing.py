  
# -*- coding: utf-8 -*-

import multiprocessing as mp
import sys
import time

class mproc():
    '''
    This is a class that spawns N new processes to do the designated job
    @initializer params :
        - N : number of processes to spawn
    
        '''

    def __init__(self, N, jobtype='workers'):
        self.pool = mp.Pool(processes=N)
        self.no_of_prcs = N
        self.STATUS = 'RUNNING'

    def apply(self, runner, *funcargs):
        
        # 입력 패러미터 확인을 위한 출력
        # print ('<pure>', funcargs, '<pure>')
        
        '''
        Applies the designated job (runner) to the spawned processes
        @params:
            - runner : function to be run
            - args : arguments to be passed to runner function
            '''
        
        try:
            self.async_results = [ self.pool.apply_async(runner, funcargs) \
                              for _ in range(self.no_of_prcs) ]
            self.STATUS = 'DONE'
        
        except Exception as e:
            print(e)
            print('Exception occured during apply_async()', __file__)
        
    def _close(self):
        self.pool.close()
        self.pool.join()
    
    def get(self, timeout=None):
        '''
            returns result when it's ready
            returns the list of results
                success : list of result
                fail    : empty list

            '''
        self._close()
        rets = [] #list of return values
        if self.STATUS == 'DONE':
            try:
                for result in self.async_results:
                    if timeout: #raise exception if the result is not ready
                        result.successful()
                    rets.append(result.get(timeout))

            #need modification
            except mp.TimeoutError as e:
                print(e)
                raise e

            except AssertionError as e:
                print(e)
                raise e
                
            except Exception as e:
                print(e)
                pass
                #raise e

        return rets
