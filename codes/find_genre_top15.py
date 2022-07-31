import pickle
import pandas as pd

"""
모델링팀 두 분이 사용중인 게임 리스트에서 공통으로 사용중인 게임을 이용하여
장르별 탑 15개의 게임을 선정하기 위해 두분이 사용중인 데이터를 비교하는 코드입니다.

장르별 탑 15개 게임 리스트는 웹서비스 구현에 사용되며 각 장르별 15개 상위 게임 VIEW 테이블을 만들어
게임 선택 화면에 API를 통해 웹페이지에서 사용 할 데이터 입니다. 
"""
def check_mh_data():
    """
    미현님 모델에 사용된 게임 리스트를 pickle을 이용하여 불러오는 함수
    Returns:
        Pandas DataFare: 미현님의 모델에서 사용중인 게임들이 담긴 Pandas DataFrame
    """
    with open ('mh_data/gamelistdf.pkl', 'rb') as fr:
        data = pickle.load(fr)
    mh_title_df = data.copy()
    
    # 게임 이름을 기준으로 중복값 제거
    mh_title_df.drop_duplicates("game")
    
    # merge에 사용할 컬럼 추가
    mh_title_df['title_join'] = mh_title_df['game']
    
    return mh_title_df

def check_sh_data():    
    """
    세현님 모델에 사용된 게임 리스트를 csv파일로 불러오는 함수
    Returns:
        Pandas DataFare: 세현님의 모델에서 사용중인 게임들이 담긴 Pandas DataFrame
    """
    # 모델링 팀(세현님)이 사용중인 게임 리스트 학인
    sh_title_df = pd.read_csv("sh_data/df_steam (1).csv", encoding='utf-8')

    # 게임 이름만 필요하므로 title만 남긴 후 게임 이름을 기준으로 중복값 제거
    sh_title_df.drop_duplicates('title')
    sh_title_df = sh_title_df[['title']]

    return sh_title_df

def merge_df():
    """_summary_:
    미현님과 세현님의 모델에서 함께 사용중인 게임 리스트를 확인하기 위해 
    merge 후 Pandas DataFrame을 리턴하는 함수

    Returns:
        Pandas DataFrame: 모델링팀의 2가지 각 모델에서 사용중인 게임들이 INNER 조건으로 merge된 데이터 프레임
    """
    mh_title_df = check_mh_data()
    sh_title_df = check_sh_data()

    # merge 조건을 INNER JOIN으로 설정하여 공통으로 사용중인 게임 리스트 찾기
    merged_df = pd.merge(mh_title_df, sh_title_df, on='title_join', how='inner')

    # 혹시 몰라 다시한번 중복값 제거
    merged_df.drop_duplicates('title', inplace=True)

    # 필요없는 컬럼 삭제
    merged_df.drop('game_newname', axis=1, inplace=True)

    # 타이틀명 변경
    mh_sh_merged_df = merged_df.rename(columns={"title":"merged_title"})

    return mh_sh_merged_df

def extract_dw_data():
    """_summary_:
    csv로 파일로 보관중이던 DW (steam_info) 데이터와
    두 모델에서 사용중인 게임 리스트를 INNER JOIN하는 함수

    Returns:
        Pandas DataFrame: DW에 적재된 게임 리스트와 두 모델에서 사용중인 게임 리스트를 비교하여 겹치는 게임들만 남긴 DataFrame
    """
    steam_info_df = pd.read_csv('steam_info.csv', low_memory=False, index_col=0)

    # JOIN을 위한 컬럼 추가
    steam_info_df['title_join'] = steam_info_df['title']

    merged_df = merge_df()
    steam_model_merged_df = pd.merge(merged_df, steam_info_df, on='title_join', how='inner')

    # 다시 한번 중복 제거
    steam_model_merged_df.drop_duplicates()

    # 장르별 구분을 위해 게임 이름과 장르정보만 남기기
    find_genre = steam_model_merged_df[['game', 'genre']]
    return find_genre

find_genre = extract_dw_data()
def find_by_genre(genre):
    """_summary_:
    특정 장르를 이용하여 그 장르에 포함되는 게임들을 조회하는 함수

    Args:
        genre (str): 스팀에서 수집된 상위 9개의 장르(Action, Adventure, Indie, Casual, Simulation, Strategy, Sports, Racing)

    Returns:
        Pandas DataFrame: 특정 장르에 해당하는 게임들의 Pandas DataFrame
    """
    df = find_genre[find_genre['genre'].str.contains(f'{genre}')]
    return df

# 모델이 사용한 게임들의 각각 장르별 DataFrame 생성
model_action_df = find_by_genre("Action")
model_indie_df = find_by_genre("Indie")
model_adventure_df = find_by_genre("Adventure")
model_casual_df = find_by_genre('Casual')
model_simulation_df = find_by_genre('Simulation')
model_strategy_df = find_by_genre('Strategy')
model_rpg_df = find_by_genre('RPG')
model_sports_df = find_by_genre('Sports')
model_racing_df = find_by_genre('Racing')

# 7월 21일 기준 장르별 상위 데이터 추가 수집하여 불러오기(석원님 수집 데이터)
steam_action_df = pd.read_csv('sw_data/action.csv',index_col=0)
steam_adventure_df = pd.read_csv('sw_data/adventure.csv',index_col=0)
steam_casual_df = pd.read_csv('sw_data/casual.csv',index_col=0)
steam_indie_df = pd.read_csv('sw_data/indie.csv',index_col=0)
steam_racing_df = pd.read_csv('sw_data/racing.csv',index_col=0)
steam_rpg_df = pd.read_csv('sw_data/rpg.csv',index_col=0)
steam_simulation_df = pd.read_csv('sw_data/simulation.csv',index_col=0)
steam_sports_df = pd.read_csv('sw_data/sports.csv',index_col=0)
steam_strategy_df = pd.read_csv('sw_data/strategy.csv',index_col=0)

# 신규 수집 게임이름과 모델 사용 게임을 게임 이름 기준으로 조인하여 상위 15개 게임만 저장
merged_action_df = pd.merge(steam_action_df, model_action_df, on='title', how='inner')
action_top15 = merged_action_df[['title', 'appid']][:15]
action_top15.to_csv('genre_top15/action_top15.csv')

merged_adventrue_df = pd.merge(steam_adventure_df, model_adventure_df, on='title', how='inner')
adventure_top15 = merged_adventrue_df[['appid', 'title']][:15]
adventure_top15.to_csv('genre_top15/adventure_top15.csv')

merged_casual_df = pd.merge(steam_casual_df, model_casual_df, on='title', how='inner')
casual_top15 = merged_casual_df[['appid', 'title']][:15]
casual_top15.to_csv('genre_top15/casual_top15.csv')

merged_indie_df = pd.merge(steam_indie_df, model_indie_df, on='title', how='inner')
indie_top15 = merged_indie_df[['appid', 'title']][:15]
indie_top15.to_csv('genre_top15/indie_top15.csv')

merged_racing_df = pd.merge(steam_racing_df, model_racing_df, on='title', how='inner')
racint_top15 = merged_racing_df[['appid', 'title']][:15]
racint_top15.to_csv('genre_top15/racing_top15.csv')

merged_rpg_df = pd.merge(steam_rpg_df, model_rpg_df, on='title', how='inner')
rpg_top15 = merged_rpg_df[['appid', 'title']][:15]
rpg_top15.to_csv('genre_top15/rpg_top15.csv')

merged_simulation_df = pd.merge(steam_simulation_df, model_simulation_df, on='title', how='inner')
simulation_top15 = merged_simulation_df[['appid', 'title']][:15]
simulation_top15.to_csv('genre_top15/simulation_top15.csv')

merged_sports_df = pd.merge(steam_sports_df, model_sports_df, on='title', how='inner')
sports_top15 = merged_sports_df[['appid', 'title']][:15]
sports_top15.to_csv('genre_top15/sports_top15.csv')

merged_strategy_df = pd.merge(steam_strategy_df, model_strategy_df, on='title', how='inner')
strategy_top15 = merged_strategy_df[['appid', 'title']][:15]
strategy_top15.to_csv('genre_top15/strategy_top15.csv')