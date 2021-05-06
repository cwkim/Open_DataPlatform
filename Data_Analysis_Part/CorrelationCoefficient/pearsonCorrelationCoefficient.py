# -*- coding:utf-8 -*-

"""
    Author : Jaekyu Lee / github : https://github.com/JaekyuLee
"""

from __future__ import print_function
import numpy as np
import pandas as pd
from pandas import DataFrame as df
import matplotlib.pyplot as plt

'''
    Car Sharing
    카코드  생성일시  차고지코드  예약번호  GPS 속도  방향각  경도
    위도  GPS 시간  배터리 잔량  거리  일일 주행거리
    전체 주행거리  RPM  DTG SIGNAL  DTG 상태  단말기 상태  주행속도
    최고속도  평균속도  ODO  주행/TDM  주행/TTS  공회전/TIS  주행연비
    순간연비  엑셀페달열림량  트로틀밸브열림량  엔진부하량  냉각수온
    흡기온도  외기온도  엔진 공기압  엔진공기량  순간연료소모량
    전기차충전량  전기차충전모드  고도  인공위성수
    Bank1-Sensor1(wide range O2S)-Lambda  Control Module Voltage
    Engine Fuel Rate  Actual Engine-Percent Torque  Cmd
    EGR and EGR err  Engine   Friction-Percent Torque 
    Catalyst Temperature Bank 1  Time Since Engine Start 
    Barometric Pressure  Ambient Air Temperature  Engine Reference Torque 
    Monitor Status since DTC cleared  Distance Traveled While MIL is Activated
    Distance Traveled since DTC cleared
'''

'''
    Car Sharing
    CAR_CD  CREATE_TIME  DEPOT_ID  RSV_NO  GPS_SPEED  GPS_ANGLE  LONGITUDE 
    LATITUDE  GPS_TIME  BATT_POWER  DRV_DISTANCE  DRV_DISTANCE_DAY
    DRV_DISTANCE_TOT  RPM  DTG SIGNAL  DTG_STATE  DEVICE_STATE  RUN_SPEED
    MAX_SPEED  AVG_SPEED  ODO  TDM  TTS  TIS  AFR
    IFR  APS  TPS  CLV  COT
    IAT  OAT  MAP  MAF  IFC
    EVC  EVM  ALTITUDE  STL_CNT
    O2S  CMV
    EFR  ATQ
    EGR  FTQ 
    CTB  EST 
    BAP  AAT  RTQ  
    MDT  DMA
    DMC
'''

def display_percent(value):
    return '{}'.format(value*100)

def readExcel(readFileName, sheetName):
    data_jan = pd.read_excel(readFileName, sheet_name=sheetName, converters={
    'percents' : display_percent})
    return data_jan

def drawGraph(graphData):
    corrgraph = graphData.plot(kind='barh', stacked=False, grid=True)
    corrgraph.figure.savefig('corr_Graph.png')
    plt.xlabel("Correlation_Coeffient")
    plt.ylabel("Columns")
    plt.title('Pearson Correlation Coeffient of Car Sharing Data(RUN_SPEED)')
    plt.show()

def writeExcel(writeFileName, sheetName, writeDf):
    writer = pd.ExcelWriter(writeFileName, engine='xlsxwriter')
    writeDf.to_excel(writer, sheet_name=sheetName)
    writer.close()


'''
corr = lambda g : g['GPS_SPEED'].corr(g['RUN_SPEED'])
re = corr(data_jan)
print("Correlation of GPS_SPEED & RUN_SPEED", re)
data_jan = data_jan.set_index('CAR_CD')
'''

if __name__ == "__main__":
    
    # Excel File Read/Write
    fileName = "DB1D5600.xlsx"
    sheetName = "11005"
    readData = readExcel(fileName, sheetName)

    # Pearson correlation coefficient 계산
    corr_with_RunSpeed = lambda x : x.corrwith(x['RUN_SPEED'])
    grouped = readData.groupby('CAR_CD')
    df1 = df(grouped.apply(corr_with_RunSpeed))

    # Draw a graph
    graph = df1.T
    graph=graph.drop(['CAR_CD'])
    drawGraph(graph)
