from flask import Flask, request, render_template, jsonify
from sqlalchemy import create_engine, text
from gensim.models.doc2vec import Doc2Vec
from queries import *
from gentleman import *


app = Flask(__name__)


@app.route('/', methods=['GET'])
def home():
    return "Hello Flask!!"

"""
게임 선택 화면
action, adventure, casual, indie, racing, rpg, simulation, sports, strategy
각 장르별 탑 15개를 미리 VIEW로 만들어 놓은 뒤 조회하는 방식으로 구현
"""
@app.route('/select/', methods=['GET'])
def select():
    action_return = get_action_top15()
    adventure_return = get_adventure_top15()
    casual_return = get_casual_top15()
    indie_return = get_casual_top15()
    racing_return = get_racing_top15()
    rpg_return = get_rpg_top15()
    simulation_return = get_simulation_top15()
    sports_return = get_sports_top15()
    strategy_return = get_strategy_top15()
    
    select_return = {"returns" :
                [{"action_top15" : action_return}, 
                {"adventure_top15" : adventure_return}, 
                {"casual_top15" : casual_return},
                {"indie_top15" : indie_return},
                {"racing_top15": racing_return},
                {"rpg_top15" : rpg_return},
                {"simulation_top15" : simulation_return},
                {"sports_top15" : sports_return},
                {"strategy_top15" : strategy_return}]}
    return jsonify(select_return)

"""
POST 방식으로 요청 받은 appid을 모델에 넣어
추천받은 게임 6개를 numpy.int64 형식으로 응답
<numpy.int64>.item() -> numpy.int64를 int형식으로 변경해 줌
"""
@app.route('/recommendation/', methods=['POST'])
def recommend_by_app_id():
    """
    methods: POST
    return: JSON
    """
    data = request.get_json()["game_list"]
    appid1 = data[0]
    appid2 = data[1]
    appid3 = data[2]
    
    # appids : 넘파이 리스트로 추천된 앱아이디 6개
    rec_games = gentleman_ver1(appid1, appid2, appid3)
    

    game_info_list = get_game_info(rec_games)

    game_json = [{ "title" : game_info["title"].replace("  ", " "), "appid" : game_info["appid"], "image_link" : game_info["image_link"]} for game_info in game_info_list ]
    appid_return = {"recomented_games" : game_json }
    print(appid_return)
    return jsonify(appid_return)

@app.route('/api/recommendation2/', methods=['POST'])
def recommend_by_app_id2():
    data = request.get_json()["game_list"]
    appid1 = data[0]
    appid2 = data[1]
    appid3 = data[2]

    rec_games2 = gentleman_ver2(appid1, appid2, appid3)
    
    game_info_list2 = get_game_info(rec_games2)
  

    game_json2 = [{ "title" : game_info["title"].replace("  ", " "), "appid" : game_info["appid"], "image_link" : game_info["image_link"]} for game_info in game_info_list2 ]
    appid_return2 = {"recomented_games2" : game_json2}
    
    return jsonify(appid_return2)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
