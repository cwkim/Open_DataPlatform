# -*- coding:utf-8 -*-

"""
    Author : Jaekyu Lee / github : https://github.com/JaekyuLee
"""

from __future__ import print_function
import numpy as np
import pandas as pd
from pandas import DataFrame as df
import matplotlib.pyplot as plt
import math

DEGREES = 180
PI = 3.141592

def translateGPSAngle(position1_LAT, position1_LONG, position2_LAT, position2_LONG) :
    Current_LAT_Radian = position1_LAT * (PI / DEGREES)
    Current_Long_Radian = position1_LONG * (PI / DEGREES)

    Destination_LAT_Radian = position2_LAT * (PI / DEGREES)
    Destination_Long_Radian = position2_LONG * (PI / DEGREES)

    radianDistance = 0
    radianDistance = math.acos(math.sin(Current_LAT_Radian)*math.sin(Destination_LAT_Radian) +
    math.cos(Current_LAT_Radian) * math.cos(Destination_LAT_Radian)*math.cos(Current_Long_Radian-
    Destination_Long_Radian))

    GPSAngle = math.acos((math.sin(Destination_LAT_Radian)-math.sin(Current_LAT_Radian)
            *math.cos(radianDistance))/(math.cos(Current_LAT_Radian)*math.sin(radianDistance)))

    resultGpsAngle = 0
    if (math.sin(Destination_Long_Radian - Current_Long_Radian)<0) :
        resultGpsAngle = GPSAngle*(DEGREES/PI)
        resultGpsAngle = 360 - resultGpsAngle
    else:
        resultGpsAngle = GPSAngle * (DEGREES/PI)
    
    return resultGpsAngle

def azimuth(param):
    if (param > 22.5) :
        print("GPS angle : %d" % 1)
        angle=1
    elif(param > 67.5) :
        print("GPS angle : %d" % 2)
        angle=2
    elif(param > 112.5) :
        print("GPS angle : %d" % 3)
        angle=3
    elif(param > 157.5) :
        print("GPS angle : %d" % 4)
        angle=4
    elif(param > 202.5) :
        print("GPS angle : %d" % 5)
        angle=5
    elif(param > 247.5) :
        print("GPS angle : %d" % 6)
        angle=6
    elif(param > 292.5) :
        print("GPS angle : %d" % 7)
        angle=7
    else:
        print("GPS angle : %d" % 0)
        angle=0
    
    return angle


if __name__ == "__main__" :
    '''
    curPosLat = 35.278167
    curPosLong = 128.715805
    desPosLat = 35.278163
    desPosLong = 128.715927
    '''
    curPosLat = 35.278331
    curPosLong = 128.716827
    desPosLat = 35.278385
    desPosLong = 128.716964

    ''' Test Value
    curPosLat = 33.362217
    curPosLong = 126.533475
    desPosLat = 42.007014
    desPosLong = 128.055775
    '''

    result = translateGPSAngle(curPosLat, curPosLong, desPosLat, desPosLong)
    print(result)

    azimuth(result)
