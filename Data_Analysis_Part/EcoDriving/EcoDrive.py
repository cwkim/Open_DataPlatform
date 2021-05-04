# -*- coding:utf-8 -*-
import pandas as pd
from datetime import timedelta


# https://www.busan.go.kr/environment/ahvehiclegas02 기준 참고
refer_data = {"DIESEL" : 284, "GASOLINE" : 250}

class EcoDrive:
    def CalcFuelconsumForIdling(self, df):
        
        save_index = df.index.tolist()

        df = df.reset_index()
        for i in range(len(df)):
            df.loc[i, "IDL_FUEL"] = round(df.loc[i, "IDLING_TIME"] * (refer_data["DIESEL"] / 600 ))
        df = df.set_index("index")

        return df

    def RecentSumIDL_FUEL(self, df, current_date):
        df = df.set_index(["index"])
        date_list = pd.date_range(end=current_date- timedelta(days=1), periods=8, freq='D')
        
        sum_value = 0
        count = 0

        for date in date_list:
            try:
                tmp = df.loc[date, "IDL_FUEL"]
                count  += 1
            except:
                tmp = 0

            sum_value += tmp

        try:
            return sum_value #sum_value / count

        except ZeroDivisionError:
            return 0

    def RecentAverageFE(self, df, current_date):
        df = df.set_index(["index"])
        date_list = pd.date_range(end=current_date- timedelta(days=1), periods=8, freq='D')
        
        sum_value = 0
        count = 0

        for date in date_list:
            try:
                tmp = df.loc[date, "FUEL_EFFICIENCY"]
                count  += 1
            except:
                tmp = 0

            sum_value += tmp

        try:
            return round(sum_value / count, 2)

        except ZeroDivisionError:
            return 0

    def RecentBestFE(self, df, offset, current_date):
        df = df.set_index(["index"])
        before_date = date - timedelta(days=offset)
        

