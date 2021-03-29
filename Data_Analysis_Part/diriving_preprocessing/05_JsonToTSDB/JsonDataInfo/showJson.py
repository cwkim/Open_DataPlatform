#!/usr/bin/env python
#-*- coding: utf-8 -*-

#from __future__ import print_function
from datetime import datetime
import sys

class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("./logs.txt", "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)  

    def flush(self):
        pass    

    def close():
        self.log.close()

def printJsonInfo(jsondict, index):
    filename = jsondict.keys()[index]
    showdict = jsondict.values()[index]

    sys.stdout.write("\n파일명        : " + filename + '_*\n\n')
    for file_path in showdict["files"]:
        sys.stdout.write(file_path+'\n')
    # 파일과 화면 출력을 동시에 실행
    sys.stdout.write("\nmetric          : " + showdict["metric"]+'\n')
    sys.stdout.write("\nMin timestamp   : " + showdict["minTS"] + ' (' + str(datetime.fromtimestamp(int(showdict["minTS"])))+ ')\n')
    sys.stdout.write("Max timestamp   : " + showdict["maxTS"] + ' (' + str(datetime.fromtimestamp(int(showdict["maxTS"])))+ ')\n')
    sys.stdout.write("\ntotal value Cnt : " + str(showdict["totalValNum"])+'\n')
    sys.stdout.write("Min value       : " + str(showdict["minVal"])+'\n')
    sys.stdout.write("Max value       : " + str(showdict["maxVal"])+'\n')
    sys.stdout.write("\ntags            : ")
    for key in showdict["tags"].keys():
        sys.stdout.write(key+" : "+showdict["tags"][key]+'\n                  ')
    sys.stdout.write('\n-----------------------------------------------------\n')


if __name__ == "__main__":
    
    import __JsonDataInfo
    JsonInfo = __JsonDataInfo.JsonDataInfo
    
    # shell에서 a 입력시 모든 종류의 파일정보 출력
    if sys.argv[1]=='a':
        num = 0
    else:
        num = int(sys.argv[1])
    num -= 1
    
    sys.stdout = Logger()
    if num == -1:
        for i in range(len(JsonInfo)):
            printJsonInfo(JsonInfo, i)
    else:
        printJsonInfo(JsonInfo, num)

  