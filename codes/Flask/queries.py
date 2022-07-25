import pymysql

gentleman = pymysql.connect(
    host="HOST",
    user="USER",
    passwd='PWD',
    db='DBNAME', 
    port=3306
)
cursor = gentleman.cursor(pymysql.cursors.DictCursor)

"""
선택화면에서 표시될 각 장르별 상위 15개의 게임을 미리 VIEW 테이블로 만들어 놓고
그 테이블을 조회한 결과를 리턴하는 함수들을 모아두었습니다.
"""
# Action top 15
def get_action_top15():
    sql = """
    SELECT appid, title, image_link
    FROM action_top15;
    """
    cursor.execute(sql)
    action_cur = cursor.fetchall()

    action = {
        "game1" : action_cur[0],
        "game2" : action_cur[1],
        "game3" : action_cur[2],
        "game4" : action_cur[3],
        "game5" : action_cur[4],
        "game6" : action_cur[5],
        "game7" : action_cur[6],
        "game8" : action_cur[7],
        "game9" : action_cur[8],
        "game10" : action_cur[9],
        "game11" : action_cur[10],
        "game12" : action_cur[11],
        "game13" : action_cur[12],
        "game14" : action_cur[13],
        "game15" : action_cur[14]
    }
    return action

# Adventure top 15
def get_adventure_top15():
    sql = """
    SELECT appid, title, image_link
    FROM adventure_top15;
    """
    cursor.execute(sql)
    adventure_cur = cursor.fetchall()

    adventure = {
        "game1" : adventure_cur[0],
        "game2" : adventure_cur[1],
        "game3" : adventure_cur[2],
        "game4" : adventure_cur[3],
        "game5" : adventure_cur[4],
        "game6" : adventure_cur[5],
        "game7" : adventure_cur[6],
        "game8" : adventure_cur[7],
        "game9" : adventure_cur[8],
        "game10" : adventure_cur[9],
        "game11" : adventure_cur[10],
        "game12" : adventure_cur[11],
        "game13" : adventure_cur[12],
        "game14" : adventure_cur[13],
        "game15" : adventure_cur[14]
    }
    return adventure

# Casual top 15
def get_casual_top15():
    sql = """
    SELECT appid, title, image_link
    FROM casual_top15;
    """
    cursor.execute(sql)
    casual_cur = cursor.fetchall()

    casual = {
        "game1" : casual_cur[0],
        "game2" : casual_cur[1],
        "game3" : casual_cur[2],
        "game4" : casual_cur[3],
        "game5" : casual_cur[4],
        "game6" : casual_cur[5],
        "game7" : casual_cur[6],
        "game8" : casual_cur[7],
        "game9" : casual_cur[8],
        "game10" : casual_cur[9],
        "game11" : casual_cur[10],
        "game12" : casual_cur[11],
        "game13" : casual_cur[12],
        "game14" : casual_cur[13],
        "game15" : casual_cur[14]
    }
    return casual


# Indie top 15
def get_indie_top15():
    sql = """
    SELECT appid, title, image_link
    FROM indie_top15;
    """
    cursor.execute(sql)
    indie_cur = cursor.fetchall()

    indie = {
        "game1" : indie_cur[0],
        "game2" : indie_cur[1],
        "game3" : indie_cur[2],
        "game4" : indie_cur[3],
        "game5" : indie_cur[4],
        "game6" : indie_cur[5],
        "game7" : indie_cur[6],
        "game8" : indie_cur[7],
        "game9" : indie_cur[8],
        "game10" : indie_cur[9],
        "game11" : indie_cur[10],
        "game12" : indie_cur[11],
        "game13" : indie_cur[12],
        "game14" : indie_cur[13],
        "game15" : indie_cur[14]
    }
    return indie

# Racing top 15
def get_racing_top15():
    sql = """
    SELECT appid, title, image_link
    FROM racing_top15;
    """
    cursor.execute(sql)
    racing_cur = cursor.fetchall()

    racing = {
        "game1" : racing_cur[0],
        "game2" : racing_cur[1],
        "game3" : racing_cur[2],
        "game4" : racing_cur[3],
        "game5" : racing_cur[4],
        "game6" : racing_cur[5],
        "game7" : racing_cur[6],
        "game8" : racing_cur[7],
        "game9" : racing_cur[8],
        "game10" : racing_cur[9],
        "game11" : racing_cur[10],
        "game12" : racing_cur[11],
        "game13" : racing_cur[12],
        "game14" : racing_cur[13],
        "game15" : racing_cur[14]
    }
    return racing

# Racing top 15
def get_racing_top15():
    sql = """
    SELECT appid, title, image_link
    FROM racing_top15;
    """
    cursor.execute(sql)
    racing_cur = cursor.fetchall()

    racing = {
        "game1" : racing_cur[0],
        "game2" : racing_cur[1],
        "game3" : racing_cur[2],
        "game4" : racing_cur[3],
        "game5" : racing_cur[4],
        "game6" : racing_cur[5],
        "game7" : racing_cur[6],
        "game8" : racing_cur[7],
        "game9" : racing_cur[8],
        "game10" : racing_cur[9],
        "game11" : racing_cur[10],
        "game12" : racing_cur[11],
        "game13" : racing_cur[12],
        "game14" : racing_cur[13],
        "game15" : racing_cur[14]
    }
    return racing

# RPG top 15
def get_rpg_top15():
    sql = """
    SELECT appid, title, image_link
    FROM rpg_top15;
    """
    cursor.execute(sql)
    rpg_cur = cursor.fetchall()

    rpg = {
        "game1" : rpg_cur[0],
        "game2" : rpg_cur[1],
        "game3" : rpg_cur[2],
        "game4" : rpg_cur[3],
        "game5" : rpg_cur[4],
        "game6" : rpg_cur[5],
        "game7" : rpg_cur[6],
        "game8" : rpg_cur[7],
        "game9" : rpg_cur[8],
        "game10" : rpg_cur[9],
        "game11" : rpg_cur[10],
        "game12" : rpg_cur[11],
        "game13" : rpg_cur[12],
        "game14" : rpg_cur[13],
        "game15" : rpg_cur[14]
    }
    return rpg

# simulation top 15
def get_simulation_top15():
    sql = """
    SELECT appid, title, image_link
    FROM simulation_top15;
    """
    cursor.execute(sql)
    simulation_cur = cursor.fetchall()

    simulation = {
        "game1" : simulation_cur[0],
        "game2" : simulation_cur[1],
        "game3" : simulation_cur[2],
        "game4" : simulation_cur[3],
        "game5" : simulation_cur[4],
        "game6" : simulation_cur[5],
        "game7" : simulation_cur[6],
        "game8" : simulation_cur[7],
        "game9" : simulation_cur[8],
        "game10" : simulation_cur[9],
        "game11" : simulation_cur[10],
        "game12" : simulation_cur[11],
        "game13" : simulation_cur[12],
        "game14" : simulation_cur[13],
        "game15" : simulation_cur[14]
    }
    return simulation

# sports top 15
def get_sports_top15():
    sql = """
    SELECT appid, title, image_link
    FROM sports_top15;
    """
    cursor.execute(sql)
    sports_cur = cursor.fetchall()

    sports = {
        "game1" : sports_cur[0],
        "game2" : sports_cur[1],
        "game3" : sports_cur[2],
        "game4" : sports_cur[3],
        "game5" : sports_cur[4],
        "game6" : sports_cur[5],
        "game7" : sports_cur[6],
        "game8" : sports_cur[7],
        "game9" : sports_cur[8],
        "game10" : sports_cur[9],
        "game11" : sports_cur[10],
        "game12" : sports_cur[11],
        "game13" : sports_cur[12],
        "game14" : sports_cur[13],
        "game15" : sports_cur[14]
    }
    return sports

# strategy top 15
def get_strategy_top15():
    sql = """
    SELECT appid, title, image_link
    FROM strategy_top15;
    """
    cursor.execute(sql)
    strategy_cur = cursor.fetchall()

    strategy = {
        "game1" : strategy_cur[0],
        "game2" : strategy_cur[1],
        "game3" : strategy_cur[2],
        "game4" : strategy_cur[3],
        "game5" : strategy_cur[4],
        "game6" : strategy_cur[5],
        "game7" : strategy_cur[6],
        "game8" : strategy_cur[7],
        "game9" : strategy_cur[8],
        "game10" : strategy_cur[9],
        "game11" : strategy_cur[10],
        "game12" : strategy_cur[11],
        "game13" : strategy_cur[12],
        "game14" : strategy_cur[13],
        "game15" : strategy_cur[14]
    }
    return strategy

"""
appid를 입력받아 title 리턴하는 함수
모델이 출력 결과를 appid로 출력하기 때문에 
appid를 이용하여 db에 조회를 하여 title을 불러옴
"""
def get_title(appid):
    sql = f"SELECT title, appid FROM steam_game_info WHERE appid = {appid};"
    cursor.execute(sql)
    action_cur = cursor.fetchall()
    title = action_cur[0]
    return title