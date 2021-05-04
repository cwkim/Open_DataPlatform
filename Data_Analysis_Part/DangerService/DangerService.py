# -*- coding:utf-8 -*-

import pandas as pd

#import danger_decision

weight_mapper = {'D':10,
                'S':10,
                'T':5,
                'U':5,
                'SL':5,
                'A':2,
                'Q':2,
                'O':2}


class DangerService:
    ''' 위험운전 서비스 스크립트 draft.
        인터페이스에 표시할 데이터를 계산하고 DB 에 저장까지 함.
        매일매일 구동해서 업데이트 함.
    '''    
    
    def data_reshape(self, raw_data):
        ''' 원본 데이터 --> 기초계산용 데이터를 위한 reshaping.
        params
            year: (string) year.
            month: (string) month.
        return
            reshaped_danger_data: (DataFrame) 
        '''
    
        total_danger = raw_data.groupby('car_id').size()
        type_danger = raw_data.groupby(['car_id','TYPE']).size()

        reshaped_danger_data = pd.DataFrame(columns = 
                          ['TOTAL_COUNT', 'A', 'Q', 'D', 'S', 'T', 'U', 'SL', 'O'])

        for i, car in enumerate(total_danger.index):
            reshaped_danger_data.loc[i, 'CAR_ID'] = car
            reshaped_danger_data.loc[i, 'TOTAL_COUNT'] = total_danger[car]

            try: reshaped_danger_data.loc[i, 'A'] = type_danger[car]['A']
            except: reshaped_danger_data.loc[i, 'A'] = 0

            try: reshaped_danger_data.loc[i, 'Q'] = type_danger[car]['Q']
            except: reshaped_danger_data.loc[i, 'Q'] = 0

            try: reshaped_danger_data.loc[i, 'D'] = type_danger[car]['D']
            except: reshaped_danger_data.loc[i, 'D'] = 0

            try: reshaped_danger_data.loc[i, 'S'] = type_danger[car]['S']
            except: reshaped_danger_data.loc[i, 'S'] = 0

            try: reshaped_danger_data.loc[i, 'T'] = type_danger[car]['T']
            except: reshaped_danger_data.loc[i, 'T'] = 0

            try: reshaped_danger_data.loc[i, 'U'] = type_danger[car]['U']
            except: reshaped_danger_data.loc[i, 'U'] = 0

            try: reshaped_danger_data.loc[i, 'SL'] = type_danger[car]['SL']
            except: reshaped_danger_data.loc[i, 'SL'] = 0

            try: reshaped_danger_data.loc[i, 'O'] = type_danger[car]['O']
            except: reshaped_danger_data.loc[i, 'O'] = 0
    
        return reshaped_danger_data


    def calc_weighted(self, reshaped):
        ''' danger_data 에 type 별 가중치를 적용.
        params
            reshaped: (DataFrame) reshaped datas.
        return
            weighted_data: (DataFrame) multiplied count data by weight.
        '''
        weighted_data = pd.DataFrame(columns=[])
        
        for col in reshaped.columns[:-1]:
            list_ = []
            for i in range(len(reshaped['car_id'])):
                value = int(reshaped[col].iloc[i]) * int(weight_mapper[col])
                list_.append(value)

            weighted_data[col] = list_
        weighted_data["car_id"] = reshaped["car_id"].to_list()
        return weighted_data
    

    def _calc_tscore(self, column):
        ''' calculate standardized t-score.
        params
            column: (Series) target column. each danger behavior types.
        return
            tscore: (Series) starndardized t-score of given column.
        '''
        
        std = column.std()
        mean = column.mean()
        zscore = (column - mean) / std
        tscore = zscore * 10 + 50
        return tscore
    

    def _calc_diff_with_mean(self, weighted):
        ''' calculate t score difference with mean.
        params
            weighted: (DataFrame) weighted scores.
        return
            diff: (Series) sum of t-scores each car.
        '''
        
        # 각 t score 계산
        diff = pd.DataFrame(columns=[])
        for col in weighted.columns[:-1]:
            #diff.append(self._calc_tscore(weighted[col]) - 50)
            result = self._calc_tscore(weighted[col]) - 50
            result = result.to_frame()
            diff = pd.concat([diff,result],axis=1)
        
        diff = diff.dropna(axis=1)
            
        return diff
    
        
    def calc_totalscore(self, weighted):
        ''' calculate sum of t-scores.
        params
            weighted: (DataFrame) weighted scores.
        return
            total_tscore: (Series) sum of t-scores each car.
        '''
        
        # 각 t score 계산
        tscores_df = pd.DataFrame(columns=[])
        for col in weighted.columns[:-1]:
            tscores_df[col] = self._calc_tscore(weighted[col])
        tscores_df = tscores_df.fillna(0)

        # 총점 계산
        tscores_df["TOTAL_SCORE"] = (tscores_df.sum(axis=1) / len(weighted.columns[:-1]))
        
        # 차량번호 표시
        tscores_df["car_id"] = weighted["car_id"].to_list()
        
        return tscores_df
    
    
    def rank(self, df, total_tscore):
        ''' make ranking using sum of t-scores.
        params
            total_tscore: (Series) calculated total tscores.
        return
            rank_data: (dict) score, rank, percent, grade data of each car.
        '''
        
        dic = {}
        max_score = total_tscore.max()
        for i, r in enumerate(total_tscore):
            dic[df['car_id'][i]] = (1 - (r / max_score)) * 100
        
        dic = sorted(dic.items(), key=lambda x:x[1], reverse=True)

        rank_data = {"car_id":{}, "SCORE":{}, "RANK":{}, "PERCENT":{}, "GRADE":{}}
        for idx, dat in enumerate(dic):
            dat = list(dat)
            #dat.append(idx)
            per = 100 * idx / len(dic)
            #dat.append(per)
            #dat.append(int(per/20) + 1)
            #dic[idx] = dat
            
            #rank_data[dic[idx][0]] = dic[idx][1:]

            rank_data["car_id"][idx] = dat[0]
            rank_data["SCORE"][idx] = dat[1]
            rank_data["RANK"][idx] = idx + 1 
            rank_data["PERCENT"][idx] = per
            rank_data["GRADE"][idx] = int(per/20) + 1
            
        rank_df = pd.DataFrame(rank_data)    

        score_max = rank_df["SCORE"].max()
        rank_df["SCORE"] = round((rank_df["SCORE"] / score_max) * 100, 1)
        rank_df["SCORE"] = rank_df["SCORE"].astype(int)
        
        return rank_df


    def state(self, ori_data):
        ''' 
            각 차량별로 상태 제시.
        '''
        car_list = ori_data["car_id"].reset_index(drop=True)  
        
        ori_data = ori_data.apply(pd.to_numeric)
        col_list = ['A', 'D', 'Q', 'S', 'T', "U", 'SL', 'O']
            
        state_df = pd.DataFrame(columns=[])

        for col in col_list:
            state_list = []
            Q3 = ori_data[col].quantile(0.75)
            Q4 = ori_data[col].quantile(0.89)
            mean = ori_data[col].mean()
            
            for i in range(len(ori_data)):
                if ori_data[col].iloc[i]  <= mean:
                    state_list.append("양호")
                elif ori_data[col].iloc[i] > mean and ori_data[col].iloc[i] <= Q3:
                    state_list.append("주의")
                elif ori_data[col].iloc[i] > Q3 and ori_data[col].iloc[i] <= Q4:
                    state_list.append("경고")
                else:
                    state_list.append("위험")
            state_df[col] = state_list
        
        state_df['car_id'] = car_list
        
        return state_df
        