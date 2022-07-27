import pandas as pd
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
import pymysql
import re
import pymongo

# MongoDB에서 불러오기
MG_USER = "user"
MG_PWD = "pwd"
MG_HOST = "host"
MG_PORT = "port"

client = pymongo.MongoClient(f"mongodb://{MG_USER}:{MG_PWD}@{MG_HOST}:{MG_PORT}")
db = client['steam']

def __get_mongodb_collection(collection_name):
    """
    :params
        collection_name : DataLake인 MongoDB에 적재하기 데이터를 위해 컬랙션을 선택
    Returns:
        collection : MongoDB에서 선택 될 컬랙션 리턴
    """
    USER = "user"
    PWD = "pwd"
    HOST = "host"
    PORT = "port"
    client = pymongo.MongoClient(f"mongodb://{USER}:{PWD}@{HOST}:{PORT}")
    db = client['steam']
    collection = db[collection_name]

    return collection

d_list = []
for d in db["steam_info"].find({},{"_id":0}):
    d_list.append(d)
df = pd.DataFrame(d_list)

df = df.drop_duplicates("appid")
df = df.dropna(axis=0)

df.loc[df["grade"] == "all", "grade"] = 1
df["grade"] = df["grade"].astype(int)

df["release_date"] = pd.to_datetime(df["release_date"], dayfirst=True, errors='coerce')

final_df = df.copy()

# 리스트 형식으로 저장된 컬럼들의 리스트를 해제하기 위한 코드
final_df['info'] = final_df['info'].apply(lambda x: ','.join(map(str, x)))
final_df['tag'] = final_df['tag'].apply(lambda x: ','.join(map(str, x)))
final_df['language_interface'] = final_df['language_interface'].apply(lambda x: ','.join(map(str, x)))
final_df['language_fullaudio'] = final_df['language_fullaudio'].apply(lambda x: ','.join(map(str, x)))
final_df['language_subtitles'] = final_df['language_subtitles'].apply(lambda x: ','.join(map(str, x)))

steam_df = final_df.dropna(axis=0)

pattern = '[^a-z^A-Z^0-9]'
steam_df['about_this_game'] = steam_df['about_this_game'].apply(lambda x : re.sub(pattern, ' ', str(x)))

pattern2 = '[^a-z^A-Z^0-9^.^,]'
steam_df['developer'] = steam_df['developer'].apply(lambda x : re.sub(pattern2, ' ', str(x)))
steam_df['publisher'] = steam_df['publisher'].apply(lambda x : re.sub(pattern2, ' ', str(x)))
steam_df['franchise'] = steam_df['franchise'].apply(lambda x : re.sub(pattern2, ' ', str(x)))
steam_df['title'] = steam_df['title'].apply(lambda x : re.sub(pattern2, ' ', str(x)))

HOST = 'host'
DB_USER = 'user'
DB_PASSWD = 'pwd'
DB_NAME = 'db'

conn = f"mysql+pymysql://{DB_USER}:{DB_PASSWD}@{HOST}/{DB_NAME}?charset=utf8"

engine = create_engine('mysql+pymysql://{DB_USER}:{DB_PASSWD}@{HOST}/{DB_NAME}?charset=utf8')

steam_df.to_sql(name='steam_game_info', con=conn, if_exists="append")