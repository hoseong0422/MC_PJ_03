import pymysql

gentleman = pymysql.connect(
    host="ec2-**-***-**-***.ap-northeast-1.compute.amazonaws.com",
    user="****",
    passwd='****',
    db='gentleman', 
    port=3306
    )
cursor = gentleman.cursor(pymysql.cursors.DictCursor)


# Action top 15
def get_action_top15():
    """
    선택화면에서 표시될 각 장르별 상위 15개의 게임을 미리 VIEW 테이블로 만들어 놓고
    그 테이블을 조회한 결과를 리턴하는 함수
    """
    sql = """
    SELECT appid, title, image_link
    FROM action_top15;
    """
    cursor.execute(sql)
    action_cur = cursor.fetchall()

    action = list(action_cur)[0:15]

    return action

# Adventure top 15
def get_adventure_top15():
    sql = """
    SELECT appid, title, image_link
    FROM adventure_top15;
    """
    cursor.execute(sql)
    adventure_cur = cursor.fetchall()

    adventure = list(adventure_cur)[0:15]
    return adventure

# Casual top 15
def get_casual_top15():
    sql = """
    SELECT appid, title, image_link
    FROM casual_top15;
    """
    cursor.execute(sql)
    casual_cur = cursor.fetchall()

    casual = list(casual_cur)[0:15]
    return casual

# Indie top 15
def get_indie_top15():
    sql = """
    SELECT appid, title, image_link
    FROM indie_top15;
    """
    cursor.execute(sql)
    indie_cur = cursor.fetchall()

    indie = list(indie_cur)[0:15]
    return indie

# Racing top 15
def get_racing_top15():
    sql = """
    SELECT appid, title, image_link
    FROM racing_top15;
    """
    cursor.execute(sql)
    racing_cur = cursor.fetchall()

    racing = list(racing_cur)[0:15]
    return racing

# RPG top 15
def get_rpg_top15():
    sql = """
    SELECT appid, title, image_link
    FROM rpg_top15;
    """
    cursor.execute(sql)
    rpg_cur = cursor.fetchall()

    rpg = list(rpg_cur)[0:15]
    return rpg

# simulation top 15
def get_simulation_top15():
    sql = """
    SELECT appid, title, image_link
    FROM simulation_top15;
    """
    cursor.execute(sql)
    simulation_cur = cursor.fetchall()

    simulation = list(simulation_cur)[0:15]
    return simulation

# sports top 15
def get_sports_top15():
    sql = """
    SELECT appid, title, image_link
    FROM sports_top15;
    """
    cursor.execute(sql)
    sports_cur = cursor.fetchall()

    sports= list(sports_cur)[0:15]
    return sports

# strategy top 15
def get_strategy_top15():
    sql = """
    SELECT appid, title, image_link
    FROM strategy_top15;
    """
    cursor.execute(sql)
    strategy_cur = cursor.fetchall()

    strategy = list(strategy_cur)[0:15]
    return strategy


def get_title(appid):
    """
        appid를 입력받아 title 리턴하는 함수
        
        모델이 출력 결과를 appid로 출력하기 때문에 appid를 이용하여 db에 조회를 하여 title을 불러옴
    """
    sql = f"""
    SELECT title, image_link, genre, release_date, video_link, descriptions,
    CASE WHEN grade = 0 THEN "등급 없음"
         WHEN grade = 1 THEN "전체 이용 가능"
         ELSE grade
         END AS grade
    FROM steam_game_info2 
    WHERE appid = {appid};"""
    cursor.execute(sql)
    action_cur = cursor.fetchall()
    title = action_cur[0]
    return title

def get_game_info(appids):
    """

    Args:
        appids (list): 조회할 appid 리스트

    Returns:
        game_list_info(list): 게임의 제목, appid, 이미지 링크가 들어있는 dict
    """
    in_str = ""
    for appid in appids:
        if appid != appids[-1]:
            in_str += str(appid) + ","
        else:
            in_str += str(appid)
    sql = f"SELECT title, appid, image_link FROM steam_game_info2 WHERE appid in ({in_str});"
    cursor.execute(sql)
    action_cur = cursor.fetchall()
    game_info_list = list(action_cur)
    return game_info_list