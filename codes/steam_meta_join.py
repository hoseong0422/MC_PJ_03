import pandas as pd
import re

# csv로 저장되어있던 steam과 metacritic 게임 정보 파일 불러오기
steam_df = pd.read_csv('steam_info.csv', low_memory=False, index_col = 0)
meta_df = pd.read_csv('preprocessed_meta_info.csv', index_col = 0)

# join을 위한 컬럼 추가하며 전체 게임이름을 소무자로 변경
steam_df['title_join'] = steam_df['title'].str.lower()
meta_df['title_join'] = meta_df['title'].str.lower()

# 게임 이름에서 알파벳과 숫자만 남기기
pattern = '[^a-z^A-Z^0-9]'
steam_df['title_join'] = steam_df['title_join'].apply(lambda x : re.sub(pattern, ' ', str(x)))
meta_df['title_join'] = meta_df['title_join'].apply(lambda x : re.sub(pattern, ' ', str(x)))

# 게임 이름 공백 제거
steam_df['title_join'] = steam_df['title_join'].str.replace(" ", "")
meta_df['title_join'] = meta_df['title_join'].str.replace(" ", "")

# 조인하였을때 구분을 위해서 각 플랫폼의 게임 이름 컬럼을 각 플랫폼에 맞게 변경
steam_df.rename(columns={'title':'steam_title'}, inplace=True)
meta_df.rename(columns={'title':'meta_title'}, inplace=True)

# title_join컬럼을 기준으로 INNER JOIN 실시
merged_df = meta_df.merge(steam_df, how="inner", on="title_join")

# appid를 기준으로 중복 제거
merged_df.drop_duplicates("appid", keep="first", inplace=True)
