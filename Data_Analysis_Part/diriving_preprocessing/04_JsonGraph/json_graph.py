# -*- coding: utf-8 -*-
# Author : ChulseoungChae, https://github.com/ChulseoungChae

from __future__ import print_function
import os
import time
import datetime
import sys
import json
from matplotlib import pyplot as plt
import argparse


def brush_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-carid", help="그래프에서 비교할 두 차의 번호")
    parser.add_argument("-fieldname", help="그래프에서 비교할 필드 이름")

    args = parser.parse_args()
    
    return args

def draw_graph(t1, t2, v1, v2, tag_name):
    plt.plot(t1,v1)
    plt.plot(t2,v2)
    plt.xlabel('Time')
    plt.ylabel('Value')
    plt.title(tag_name)
    plt.legend(['A', 'B'])
    # Ubuntu에서 UserWarning 발생시 "sudo apt-get install python-tk" or "sudo apt-get install python3-tk"
    plt.show()
    return 0


if __name__ == "__main__":
    _args_ = brush_argparse()
    carid = _args_.carid
    carid = carid.split(',')
    carid1 = int(carid[0])-1
    carid2 = int(carid[1])-1
    fieldname = int(_args_.fieldname)-1

    import __dataInfo
    datalist = __dataInfo.datalist

    idlist=[]
    fieldlist=[]
    for data in datalist:
        if not data["tags"]["fieldname"] in fieldlist:
            fieldlist.append(data["tags"]["fieldname"])
        if not data["tags"]["VEHICLE_NUM"] in idlist:
            idlist.append(data["tags"]["VEHICLE_NUM"])

    tag_name = fieldlist[fieldname]
    
    for d in datalist:
        if d["tags"]["fieldname"] == fieldlist[fieldname]:
            if d["tags"]["VEHICLE_NUM"] == idlist[carid1]:
                time_list1 = d["tslist"]
                value_list1 = d["vallist"]
                c=1
            elif d["tags"]["VEHICLE_NUM"] == idlist[carid2]:
                time_list2 = d["tslist"]
                value_list2 = d["vallist"]

    draw_graph(time_list1, time_list2, value_list1, value_list2, tag_name)