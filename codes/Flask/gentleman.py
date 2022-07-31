from gensim.models.doc2vec import Doc2Vec
import pandas as pd

def gentleman_ver1(game1, game2, game3):
    """
    Args:
        game1 (str): appid1
        game2 (str): appid2
        game3 (str): appid3
        선택 페이지에서 사용자가 선택한 게임 3개를 넘겨 받아서 모델에 전달
    Returns:
        list: 모델의 추천 결과 게임 6개
    """
    # with open('models/appiddoc2vec.model', 'rb') as f:
    #     model = pickle.load(f)
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
    # 추천 리스트 간 중복되는 게임이 없는 경우 : 각 추천리스트에서 2개씩 추출
    if len(df) == 30:
        return [x[0] for x in model.docvecs.most_similar([game1], topn=2)] + [x[0] for x in
                                                                              model.docvecs.most_similar([game2],
                                                                                                         topn=2)] + [
                   x[0] for x in model.docvecs.most_similar([game3], topn=2)]
    # 추천 리스트 간 중복되는 게임 있는 경우 : 유사도 평균 내서 상위 6개 추출
    else:
        df.fillna(0, inplace=True)
        df['mean'] = df.mean(axis=1, numeric_only=True)
        df.sort_values('mean', ascending=False, inplace=True)
        return list(df['game'])[:6]
def recommend_game(appid, top_n):
    """
    Args:
        appid (str): appid(스팀의 앱아이디)
        top_n (int): top_n(추천 받을 게임 갯수)
        저장된 모델에서 추천 받아
    Returns:
        rec_game: 유사도로 추천 된 게임 DataFrame
    """
    similarity_rate2 = pd.read_pickle('models/gamedata-001.pkl')  # 피클로 저장된 파일 불러오기
    rec_game=similarity_rate2[appid].reset_index()
    rec_game = rec_game.rename(columns={"appid": "game",appid:'similarity'})
    rec_game=rec_game.sort_values(by='similarity',ascending=False).head(top_n)
    return rec_game
def gentleman_ver2(game1, game2, game3):
    """
    Args:
        game1 (str): appid1
        game2 (str): appid2
        game3 (str): appid3
        선택 페이지에서 사용자가 선택한 게임 3개를 넘겨 받아서 모델에 전달
    Returns:
        list: 모델의 추천 결과 게임 6개
    """
    d1 = pd.DataFrame(recommend_game(game1, top_n=10), columns=['game', 'similarity']) # game1 추천목록 & 유사도
    d2 = pd.DataFrame(recommend_game(game2, top_n=10), columns=['game', 'similarity']) # game2 추천목록 & 유사도
    d3 = pd.DataFrame(recommend_game(game3, top_n=10), columns=['game', 'similarity']) # game3 추천목록 & 유사도
    df = pd.merge(d1, d2, on='game', how='outer') # d1, d2 merge
    df = pd.merge(df, d3, on='game', how='outer') # d2, d3 merge
    for game in df['game']: # ['game'] 행 하나씩 돌면서
        if game in [game1, game2, game3]: # game이 game1, game2, game3에 해당하는 경우
            drop_index = df[df['game']==game].index
            df.drop(drop_index, axis='index', inplace=True) # 해당 행 drop
    # 추천 리스트 간 중복되는 게임이 없는 경우 각 추천리스트에서 2개씩 뽑음
    if len(df) == 30:
        return [x for x in recommend_game(game1, top_n=2)['game'].unique()] + [x for x in recommend_game(game2, top_n=2)['game'].unique()] + [x for x in recommend_game(game3, top_n=2)['game'].unique()]
    # 유사도 평균 내서 상위 6개 추출
    else :
        df.fillna(0, inplace=True)
        df['mean'] = df.mean(axis=1, numeric_only=True)
        df.sort_values('mean', ascending=False, inplace=True)
        return list(df['game'][:6])