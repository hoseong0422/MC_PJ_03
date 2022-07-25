from gensim.models.doc2vec import Doc2Vec
import pandas as pd
import numpy as np

def gentleman_ver1(game1, game2, game3):
    model = Doc2Vec.load('models/appiddoc2vec.model')
    d1 = pd.DataFrame(model.docvecs.most_similar([game1], topn=10), columns=['game', 'similarity1']) # game1 추천목록 & 유사도
    d2 = pd.DataFrame(model.docvecs.most_similar([game2], topn=10), columns=['game', 'similarity2']) # game2 추천목록 & 유사도
    d3 = pd.DataFrame(model.docvecs.most_similar([game3], topn=10), columns=['game', 'similarity3']) # game3 추천목록 & 유사도
    df = pd.merge(d1, d2, on='game', how='outer') # d1, d2 merge
    df = pd.merge(df, d3, on='game', how='outer') # d2, d3 merge

    for game in df['game']: # ['game'] 행 하나씩 돌면서 
        if game in [game1, game2, game3]: # game이 game1, game2,m game3에 해당하는 경우
            drop_index = df[df['game']==game].index
            df.drop(drop_index, axis='index', inplace=True) # 해당 행 drop

    # 추천 리스트 간 중복되는 게임이 없는 경우 각 추천리스트에서 2개씩 뽑음
    if len(df) == 30: 
        return [x[0] for x in model.docvecs.most_similar([game1], topn=2)] + [x[0] for x in model.docvecs.most_similar([game2], topn=2)] + [x[0] for x in model.docvecs.most_similar([game3], topn=2)]
    # 유사도 평균 내서 상위 6개 추출
    else : 
        df.fillna(0, inplace=True)
        df['mean'] = df.mean(axis=1, numeric_only=True)
        df.sort_values('mean', ascending=False, inplace=True)
        return list(df['game'])[:6]