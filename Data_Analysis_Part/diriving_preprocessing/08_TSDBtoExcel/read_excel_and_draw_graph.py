# -*-coding:utf-8-*-
import pandas as pd

def read_file(file_name):
    print('==================== Start read file : %s ====================' %file_name)    
    df = pd.read_excel(file_name, encoding='utf8')

    print('==================== Complete read file ====================')    
    return df

if __name__ == '__main__':
    file_name = 'result.xlsx'

    df = read_file(file_name)

    print(df)
